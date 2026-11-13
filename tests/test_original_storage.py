"""Original downloader defaults, references and explicit offline migration regressions."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace as NS
import test_cached_resend as fixtures
from test_shared_media import Redis
from tg_forwarder.canonical_media import filename, relative, photo_unique_id, truncate_filename, validate_title, fingerprint
from tg_forwarder.shared_media import SharedMedia, SharedUnavailable, digest
from tg_forwarder.control.engine import Engine, RateGate
from tg_forwarder.control.store import Store, digest as cache_digest
from tg_forwarder.storage_migration import Migration


def selected(**meta):
    return {'chat':5268216341,'id':54,'kind':'photo','media_id':'6154650511710734502',
            'archive_meta':{'title':'+852_R18研究所','datetime':'2025-08-01T00:10:00+00:00',
                'photo_id':6154650511710734502,**meta}}


class OriginalFormatterTests(unittest.TestCase):
    def test_observed_photo_golden(self):
        self.assertEqual(photo_unique_id(6154650511710734502),'AgADpsgxGwi2aVU')
        self.assertEqual(str(relative(selected())),'+852_R18研究所/2025_08/54 - AgADpsgxGwi2aVU.jpg')

    def test_document_name_and_missing_name_are_original_defaults(self):
        item=selected(filename='path/m_puppy_bb.zip');item.update(id=14583,kind='video')
        self.assertEqual(filename(item),'14583 - m_puppy_bb.zip')
        item['archive_meta'].update(filename=None,extension='.mp4')
        self.assertEqual(filename(item),'14583.mp4')

    def test_unicode_spaces_and_exact_sanitizer(self):
        self.assertEqual(validate_title('a/\\:*?"<>|\n🍵 研究\t\r'),'a__________🍵 研究\t\r')
        item=selected(title='😀 群組 +852',filename='e\u0301.'+'e\u0301');item['kind']='video'
        self.assertEqual(filename(item),'54 - é.e\u0301')
        self.assertTrue(str(relative(item)).startswith('😀 群組 +852/'))

    def test_utf8_truncation_preserves_extension_exactly(self):
        path='群組/'+'e\u0301'*300+'.mp4'
        actual=truncate_filename(path)
        self.assertEqual(actual,'群組/'+'é'*113+'.mp4')
        self.assertEqual(len(Path(actual).name.encode()),230)

    def test_month_boundary_and_no_date(self):
        item=selected(datetime='2025-08-01T00:10:00+00:00')
        self.assertEqual(relative(item,'America/Los_Angeles').parts[1],'2025_07')
        self.assertEqual(relative(item,'UTC').parts[1],'2025_08')
        item['archive_meta']['datetime']=None
        self.assertEqual(relative(item).parts[1],'0')

    def test_linux_basename_and_extension_fallback(self):
        item=selected(filename='../name',extension='.mp4');item['kind']='video'
        self.assertEqual(filename(item),'54 - name.mp4')
        item['archive_meta']['filename']='dir/name\\part.zip'
        self.assertEqual(filename(item),'54 - name_part.zip')


class CanonicalFlowTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=fixtures.CachedResendTests.asyncSetUp
    asyncTearDown=fixtures.CachedResendTests.asyncTearDown
    original=fixtures.CachedResendTests.original
    forbid_source=fixtures.CachedResendTests.forbid_source
    assert_no_source=fixtures.CachedResendTests.assert_no_source
    click=fixtures.CachedResendTests.click
    button=fixtures.CachedResendTests.button
    execute=fixtures.CachedResendTests.execute

    def enable_bridge(self):
        for msg in self.client.main:msg.photo=NS(id=80000+msg.id)
        bridge=SharedMedia(Redis(),self.root/'shared',account_rate_key='offline',min_free_bytes=0,rate_interval=.001)
        self.engine.bridge=bridge;self.engine.bot_id=6317800517
        bridge.register_root('control',self.config.root)
        return bridge

    async def test_control_has_one_persistent_file_and_reference_legacy_can_read(self):
        bridge=self.enable_bridge()
        row=await self.original(1)
        item=json.loads(row['payload'])['items'][0]
        identity=[str(item['chat']),item['id'],item['media_id']]
        entry=json.loads(await bridge.redis.get(bridge.prefix+'source:'+digest(identity)))
        self.assertEqual(entry['v'],2)
        path=await bridge.cached(identity)
        self.assertEqual(path.read_bytes(),b'data')
        self.assertFalse(list(bridge.root.glob('[0-9a-f]'*64)))
        self.assertEqual(len(list(self.config.root.rglob('*.jpg'))),1)
        other=SharedMedia(bridge.redis,self.root/'shared',account_rate_key='offline',min_free_bytes=0,
                          canonical_roots={'control':self.config.root},rate_interval=.001)
        spool=self.root/'spool.jpg'
        await other.materialize(identity,spool)
        self.assertEqual(spool.read_bytes(),b'data');spool.unlink()
        self.assertEqual(path.read_bytes(),b'data')
        self.forbid_source();await self.click(self.button())
        self.assertEqual((await self.execute())['state'],'done');self.assert_no_source()

    async def test_existing_original_destination_skips_download_without_cache_row(self):
        row=await self.original(1)
        payload=json.loads(row['payload']);path=self.engine.archive.file(payload['snapshot'],payload['items'][0])
        self.store.db.execute('DELETE FROM cache');self.store.set_task(row['id'],state='queued',due=0)
        self.forbid_source()
        self.assertEqual((await self.execute())['state'],'done');self.assert_no_source()
        self.assertEqual(path.read_bytes(),b'data')

    async def test_reference_hash_tamper_missing_and_root_mapping_fail_closed(self):
        bridge=self.enable_bridge();row=await self.original(1)
        i=json.loads(row['payload'])['items'][0];identity=[str(i['chat']),i['id'],i['media_id']]
        path=await bridge.cached(identity);path.write_bytes(b'evil')
        with self.assertRaises(SharedUnavailable):await bridge.cached(identity)
        path.unlink();self.assertIsNone(await bridge.cached(identity))
        raw=json.loads(await bridge.redis.get(bridge.prefix+'source:'+digest(identity)))
        raw['path']='../escape'
        await bridge.redis.set(bridge.prefix+'source:'+digest(identity),json.dumps(raw))
        with self.assertRaises(ValueError):await bridge.cached(identity)
        raw['root']='unconfigured';raw['path']='valid.jpg'
        await bridge.redis.set(bridge.prefix+'source:'+digest(identity),json.dumps(raw))
        with self.assertRaises(SharedUnavailable):await bridge.cached(identity)


class LegacyOwnedGCTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_gc_checks_index_age_lease_and_never_canonical(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);redis=Redis();bridge=SharedMedia(redis,root/'shared',account_rate_key='x',min_free_bytes=0)
            source=root/'file';source.write_bytes(b'data');identity=['5268216341',54,'9']
            target=await bridge.publish(identity,source)
            self.assertEqual((await bridge.gc_owned([identity]))[0]['state'],'indexed')
            redis.data.pop(bridge.prefix+'source:'+digest(identity))
            self.assertEqual((await bridge.gc_owned([identity]))[0]['state'],'young')
            marker=bridge.root/'.metadata'/(digest(identity)+'.json')
            entry=json.loads(marker.read_text());entry['created']=time.time()-100000
            marker.write_text(json.dumps(entry))
            await redis.set(bridge.prefix+'maintenance:'+digest(identity),'reader')
            self.assertEqual((await bridge.gc_owned([identity]))[0]['state'],'leased')
            redis.data.pop(bridge.prefix+'maintenance:'+digest(identity))
            self.assertEqual((await bridge.gc_owned([identity]))[0]['state'],'eligible')
            with self.assertRaises(SharedUnavailable):await bridge.gc_owned([identity],apply=True)
            self.assertEqual((await bridge.gc_owned([identity],apply=True,quiescent=True))[0]['state'],'retired')
            self.assertFalse(target.exists());self.assertEqual(source.read_bytes(),b'data')


class MigrationTests(unittest.IsolatedAsyncioTestCase):
    def setup_fixture(self, root):
        root=Path(root);oldroot=root/'old';sharedroot=root/'shared';canonical=root/'TG_Download'
        for p in (oldroot,sharedroot,canonical):p.mkdir()
        db=root/'state.db';store=Store(db)
        media=selected();media['kind']='video';media['archive_meta'].update(filename='original.mp4',photo_id=None)
        media.update(size=4,album='',section='main',date='2025-08-01',caption='',protected=False)
        payload={'action':'download','target':None,'items':[media],'snapshot':{'main':[media]}}
        task=store.create_task(5268216341,5268216341,100,payload);store.set_task(task['id'],state='done')
        old=oldroot/'old.mp4';old.write_bytes(b'data');old.chmod(0o600)
        identity=[str(media['chat']),media['id'],media['media_id']]
        shared=sharedroot/digest(identity);shared.write_bytes(b'data');shared.chmod(0o600)
        key=cache_digest([media['chat'],media['id'],media['media_id']])
        store.db.execute('INSERT INTO cache VALUES(?,?,?)',(key,str(old),4))
        manifest=oldroot/('manifest_'+task['id']+'.json');manifest.write_text(json.dumps({'path':str(old),'items':[{'path':'old.mp4'}]}));manifest.chmod(0o600)
        plan={'version':1,'owner_uid':5268216341,'db':str(db),'old_root':str(oldroot),
            'canonical_root':str(canonical),'shared_root':str(sharedroot),'old_archive':str(old),
            'shared_object':str(shared),'identity':identity,'sha256':hashlib.sha256(b'data').hexdigest(),
            'selected':media,'task_ids':[task['id']],'manifests':[str(manifest)],'timezone':'UTC'}
        before=[tuple(r) for r in store.db.execute('SELECT * FROM tasks')];store.close()
        bridge=SharedMedia(Redis(),sharedroot,account_rate_key='offline',min_free_bytes=0,canonical_roots={'control':canonical})
        return Migration(plan),bridge,before

    async def test_dryrun_has_no_changes_and_apply_retains_tasks_and_one_payload(self):
        with tempfile.TemporaryDirectory() as temp:
            m,bridge,before=self.setup_fixture(temp)
            result=await m.run();self.assertFalse(result['apply']);self.assertFalse(m.journal.exists());self.assertFalse(m.destination.exists())
            result=await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge)
            self.assertTrue(result['retired']);self.assertFalse(m.old.exists());self.assertFalse(m.shared.exists())
            self.assertEqual(m.destination.read_bytes(),b'data')
            self.assertEqual(await bridge.cached(m.identity),m.destination)
            store=Store(m.db)
            self.assertEqual([tuple(r) for r in store.db.execute('SELECT * FROM tasks')],before)
            self.assertEqual(store.db.execute('SELECT path FROM cache').fetchone()[0],str(m.destination));store.close()
            self.assertFalse(m.manifests[0].exists())
            self.assertNotIn('old.mp4',m.relocated_manifests[m.manifests[0]].read_text())
            await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge)

    async def test_every_journal_phase_recovers_without_redownload(self):
        for phase in ('validated','published','db_updated','metadata_relocated','manifests_updated','index_updated','verified','retired'):
            with self.subTest(phase=phase),tempfile.TemporaryDirectory() as temp:
                m,bridge,_=self.setup_fixture(temp)
                with self.assertRaises(RuntimeError):
                    await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge,crash_after=phase)
                self.assertTrue(any(p.exists() for p in (m.old,m.shared,m.destination)))
                m=Migration(m.plan)
                await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge)
                self.assertEqual(m.destination.read_bytes(),b'data')

    async def test_hash_collision_symlink_and_live_lock_fail_without_retirement(self):
        with tempfile.TemporaryDirectory() as temp:
            m,bridge,_=self.setup_fixture(temp)
            m.destination.parent.mkdir(parents=True)
            m.destination.write_bytes(b'evil')
            with self.assertRaises(ValueError):await m.run(apply=True,quiescent=True,bridge=bridge)
            self.assertEqual(m.old.read_bytes(),b'data');m.destination.unlink()
            m.destination.symlink_to(m.old)
            with self.assertRaises(ValueError):await m.run()
            m.destination.unlink()
            import fcntl
            with open(str(m.db)+'.lock','a') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                with self.assertRaises(BlockingIOError):await m.run(apply=True,quiescent=True,bridge=bridge)
            self.assertTrue(m.old.exists());self.assertTrue(m.shared.exists())


class MigrationPathFieldTests(unittest.TestCase):
    def test_only_path_fields_change_not_captions_or_original_names(self):
        from tg_forwarder.storage_migration import rewrite
        value={'path':'old.mp4','caption':'old.mp4','archive_meta':{'filename':'old.mp4'},'items':[{'path':'old.mp4'}]}
        changed=rewrite(value,{'old.mp4':'new.mp4'})
        self.assertEqual(changed['path'],'new.mp4')
        self.assertEqual(changed['caption'],'old.mp4')
        self.assertEqual(changed['archive_meta']['filename'],'old.mp4')
        self.assertEqual(changed['items'][0]['path'],'new.mp4')


class UpstreamHelperComparisonTests(unittest.TestCase):
    def test_pinned_upstream_helpers_match_adapter_and_license(self):
        import importlib.util
        path=Path(__file__).parent/'fixtures'/'original_downloader_format.py'
        spec=importlib.util.spec_from_file_location('pinned_original_format',path)
        original=importlib.util.module_from_spec(spec);spec.loader.exec_module(original)
        for value in ('+852_R18研究所','A / \\ : * ? " < > |\n😀\t\r','e\u0301'*200+'.mP4','folder/..hidden.longEXT'):
            self.assertEqual(validate_title(value),original.validate_title(value))
            self.assertEqual(truncate_filename(value),original.truncate_filename(value))
        notice=(Path(__file__).parents[1]/'docs/licenses/tangyoha-downloader-MIT.txt').read_text()
        self.assertIn('MIT License',notice);self.assertIn('Copyright (c) 2019 Dineshkarthik R',notice)
        self.assertIn('fe7ccdc2cc5fce5b04a464bc66cfb1266bf7ef45',path.read_text())


class MigrationNoCopyFallbackTests(unittest.IsolatedAsyncioTestCase):
    setup_fixture=MigrationTests.setup_fixture
    async def test_cross_filesystem_publish_never_copies_or_retires(self):
        import errno
        with tempfile.TemporaryDirectory() as temp:
            m,bridge,_=self.setup_fixture(temp)
            with patch('tg_forwarder.storage_migration.os.link',side_effect=OSError(errno.EXDEV,'different mounts')):
                with self.assertRaises(OSError):await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge)
            self.assertFalse(m.destination.exists());self.assertTrue(m.old.exists());self.assertTrue(m.shared.exists())


class MetadataRelocationTests(unittest.IsolatedAsyncioTestCase):
    setup_fixture=MigrationTests.setup_fixture

    async def test_nonretirement_preserves_old_bytes_and_relocated_metadata_is_private(self):
        with tempfile.TemporaryDirectory() as temp:
            m,bridge,_=self.setup_fixture(temp)
            old=m.manifests[0];original=old.read_bytes()
            await m.run(apply=True,quiescent=True,bridge=bridge)
            target=m.relocated_manifests[old]
            self.assertEqual(old.read_bytes(),original)
            self.assertEqual(target.parent,m.root/'.metadata')
            self.assertEqual(target.name,old.name)
            self.assertEqual(target.stat().st_mode & 0o777,0o600)
            self.assertEqual(json.loads(m.journal.read_text())['phase'],'verified')
            self.assertTrue(m.old.exists());self.assertTrue(m.shared.exists())
            m=Migration(m.plan)
            await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge)
            self.assertFalse(old.exists());self.assertTrue(target.exists())
            m.old_root.rmdir()  # Operator removes the now-empty root, not the tool.
            m=Migration(m.plan)
            await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge)
            self.assertTrue(target.exists());self.assertFalse(m.old_root.exists())

    async def test_manifest_collision_change_symlink_and_missing_recovery_proof_rejected(self):
        for mode in ('collision','symlink','changed-original','missing-proof'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as temp:
                m,bridge,_=self.setup_fixture(temp)
                old=m.manifests[0];target=m.relocated_manifests[old]
                if mode=='collision':
                    target.parent.mkdir();target.write_text('{"collision":true}');target.chmod(0o600)
                elif mode=='symlink':
                    target.parent.mkdir();target.symlink_to(old)
                elif mode=='changed-original':
                    await m.run(apply=True,quiescent=True,bridge=bridge)
                    old.write_text('{"changed":true}')
                else:
                    await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge)
                    m.journal.unlink()
                with self.assertRaises(ValueError):Migration(m.plan)
                self.assertTrue(m.destination.exists() or m.old.exists())

    async def test_only_planned_empty_ancestors_removed_nonempty_and_root_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            m,bridge,_=self.setup_fixture(temp)
            old=m.old;manifest=m.manifests[0]
            ancestor=m.old_root/'post';leaf=ancestor/'main';leaf.mkdir(parents=True)
            moved=leaf/old.name;old.rename(moved)
            moved_manifest=ancestor/manifest.name;manifest.rename(moved_manifest)
            store=Store(m.db);store.db.execute('UPDATE cache SET path=?',(str(moved),));store.close()
            value=json.loads(moved_manifest.read_text());value['path']=str(moved);value['items'][0]['path']=str(moved.relative_to(m.old_root))
            moved_manifest.write_text(json.dumps(value))
            plan={**m.plan,'old_archive':str(moved),'manifests':[str(moved_manifest)],'prune_dirs':[str(leaf),str(ancestor)]}
            m=Migration(plan)
            unrelated=m.old_root/'untouched';unrelated.mkdir()
            await m.run(apply=True,quiescent=True,retire_old=True,bridge=bridge)
            self.assertFalse(ancestor.exists());self.assertTrue(unrelated.is_dir());self.assertTrue(m.old_root.is_dir())
            with self.assertRaises(ValueError):Migration({**plan,'prune_dirs':[str(unrelated)]})
            with self.assertRaises(ValueError):Migration({**plan,'prune_dirs':[str(m.old_root)]})


class MigrationCLIEnvironmentTests(unittest.TestCase):
    def test_apply_loads_protected_env_without_override_or_client_store_owner(self):
        import contextlib
        import io
        from tg_forwarder import storage_migration
        from tg_forwarder.control import runtime
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);env=root/'control.env';env.write_text('REDIS_HOSTNAME=fixture-redis\nREDIS_PORT=6380\nREDIS_PASSWORD=private-fixture-value\n')
            env.chmod(0o600);plan=root/'plan.json';plan.write_text('{}')
            instance=NS(run=AsyncMock(return_value={'apply':True}))
            async def factory():
                self.assertEqual(os.environ['REDIS_HOSTNAME'],'fixture-redis')
                self.assertEqual(os.environ['REDIS_PORT'],'6390')
                self.assertEqual(os.environ['REDIS_PASSWORD'],'private-fixture-value')
                return NS()
            output=io.StringIO()
            with patch.dict(os.environ,{'CONTROL_ENV_FILE':str(env),'REDIS_PORT':'6390'},clear=True), \
                 patch('sys.argv',['storage_migration','--plan',str(plan),'--apply','--quiescent']), \
                 patch.object(storage_migration,'Migration',return_value=instance), \
                 patch.object(storage_migration,'configured_bridge',side_effect=factory) as configured, \
                 patch.object(runtime,'client_from_env',side_effect=AssertionError('no native client')) as client, \
                 patch.object(runtime,'Store',side_effect=AssertionError('no new Store owner')) as state, \
                 contextlib.redirect_stdout(output):
                storage_migration.cli()
                configured.assert_awaited_once();client.assert_not_called();state.assert_not_called()
            self.assertNotIn('private-fixture-value',output.getvalue())

    def test_dryrun_does_not_read_env_or_configure_bridge(self):
        import contextlib
        import io
        from tg_forwarder import storage_migration
        from tg_forwarder.control import runtime
        with tempfile.TemporaryDirectory() as temp:
            plan=Path(temp)/'plan.json';plan.write_text('{}')
            instance=NS(run=AsyncMock(return_value={'apply':False}))
            with patch.dict(os.environ,{'CONTROL_ENV_FILE':'/nonexistent/protected-env'},clear=True), \
                 patch('sys.argv',['storage_migration','--plan',str(plan)]), \
                 patch.object(storage_migration,'Migration',return_value=instance), \
                 patch.object(storage_migration,'configured_bridge',AsyncMock()) as configured, \
                 patch.object(runtime,'environment',side_effect=AssertionError('dry-run must not read secrets')) as environment, \
                 contextlib.redirect_stdout(io.StringIO()):
                storage_migration.cli()
                configured.assert_not_called();environment.assert_not_called()


class LegacyTransientWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def message(self):
        msg=fixtures.FakeClient().main[0]
        msg.photo=NS(id=80000+msg.id);msg.document=None;msg.text=msg.message;msg.file.mime_type='image/jpeg'
        async def download(path):
            Path(path).write_bytes(b'data');return str(path)
        msg.download_media=AsyncMock(side_effect=download)
        return msg

    async def snapshot(self,root,bridge,msg):
        from tg_forwarder.processing import snapshot
        async def api(fn,**kwargs):return await fn()
        with patch.object(snapshot,'configured_bridge',AsyncMock(return_value=bridge)):
            return await snapshot.snapshot_media_message(msg,{'chat_id':msg.chat_id,'msg_id':msg.id},
                'new',root/'spool',asyncio.Semaphore(1),api,lambda _:None)

    async def test_fresh_legacy_download_send_cleanup_leaves_no_shared_payload_or_spool_index(self):
        from tg_forwarder.core.utils import cleanup_files
        from tg_forwarder import shared_media
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);redis=Redis()
            bridge=SharedMedia(redis,root/'shared',account_rate_key='offline',min_free_bytes=0,
                persistent_cache_enabled=False,rate_interval=.001)
            msg=self.message()
            with patch.object(bridge,'_publish_file',side_effect=AssertionError('persistent copy forbidden')):
                snap=await self.snapshot(root,bridge,msg)
            msg.download_media.assert_awaited_once()
            send=AsyncMock(return_value=NS(sent_message_id=901,delivery_kind='media'))
            with patch.object(shared_media,'configured_bridge',AsyncMock(return_value=bridge)), \
                 patch.dict(os.environ,{'MEDIA_DELIVERY_IDENTITY':'fixture-legacy-bot'},clear=True):
                await shared_media.legacy_send(send,[snap],5268216341,'caption',{'sender_id':5268216341},'new')
            send.assert_awaited_once()
            cleanup_files([snap['path']],'new',msg.id,lambda _:None)
            self.assertFalse(Path(snap['path']).exists())
            self.assertFalse(bridge.root.exists())
            self.assertFalse(any(':source:' in key for key in redis.data))
            self.assertTrue(any(':done:' in key for key in redis.data))

    async def test_disabled_writer_still_reuses_canonical_reference_and_preserves_it_after_cleanup(self):
        from tg_forwarder.core.utils import cleanup_files
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);canonical=root/'TG_Download';canonical.mkdir()
            file=canonical/'photo.jpg';file.write_bytes(b'data');file.chmod(0o600)
            bridge=SharedMedia(Redis(),root/'shared',account_rate_key='offline',min_free_bytes=0,
                persistent_cache_enabled=False,canonical_roots={'control':canonical})
            msg=self.message();identity=[str(msg.chat_id),msg.id,str(msg.photo.id)]
            await bridge.publish_reference(identity,file)
            msg.download_media.side_effect=AssertionError('canonical hit must not redownload')
            snap=await self.snapshot(root,bridge,msg)
            msg.download_media.assert_not_awaited()
            cleanup_files([snap['path']],'new',msg.id,lambda _:None)
            self.assertEqual(file.read_bytes(),b'data');self.assertFalse(bridge.root.exists())
            self.assertEqual(json.loads(await bridge.redis.get(bridge.prefix+'source:'+digest(identity)))['v'],2)

    async def test_disabled_writer_preserves_old_v1_lookup_and_materialization(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);redis=Redis();source=root/'source';source.write_bytes(b'data')
            writer=SharedMedia(redis,root/'shared',account_rate_key='offline',min_free_bytes=0,persistent_cache_enabled=True)
            identity=['5268216341',54,'9'];old=await writer.publish(identity,source)
            reader=SharedMedia(redis,root/'shared',account_rate_key='offline',min_free_bytes=0,persistent_cache_enabled=False)
            self.assertEqual(await reader.cached(identity),old)
            output=root/'spool';await reader.materialize(identity,output)
            self.assertEqual(output.read_bytes(),b'data');output.unlink();self.assertTrue(old.exists())

    async def test_configured_bridge_defaults_disabled_explicit_optin_and_invalid_flag(self):
        from tg_forwarder import shared_media
        with tempfile.TemporaryDirectory() as temp:
            env={'MEDIA_SHARED_ENABLED':'true','MEDIA_SHARED_ROOT':temp,'MEDIA_ACCOUNT_RATE_KEY':'fixture',
                 'REDIS_HOSTNAME':'offline-fixture'}
            with patch.dict(os.environ,env,clear=True),patch('redis.asyncio.Redis',return_value=Redis()):
                self.assertFalse((await shared_media.configured_bridge()).persistent_cache_enabled)
                os.environ['MEDIA_SHARED_PERSISTENT_CACHE_ENABLED']='true'
                self.assertTrue((await shared_media.configured_bridge()).persistent_cache_enabled)
                os.environ['MEDIA_SHARED_PERSISTENT_CACHE_ENABLED']='not-a-boolean'
                with self.assertRaises(ValueError):await shared_media.configured_bridge()
