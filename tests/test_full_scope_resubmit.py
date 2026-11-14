"""Offline reproduction of 31-photo / 22-item planning truncation; no sockets."""
import copy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import test_source_archive_group as grouping
import test_media_workbench as source
import test_inbound_photos as inbound
from tg_forwarder.control.input import SourceRef
from tg_forwarder.control.store import digest


class FullScopeTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=grouping.MainArchiveTests.asyncSetUp
    asyncTearDown=grouping.MainArchiveTests.asyncTearDown
    transport=grouping.MainArchiveTests.transport
    forbid_source=grouping.MainArchiveTests.forbid_source
    source=grouping.MainArchiveTests.source

    async def fixture(self):
        self.source()
        self.client.main=[source.message(source.CHANNEL,4739+n,1) for n in range(10)]
        self.client.replies=[source.message(source.GROUP,23508+n,100+n//10,'video' if n>=36 else 'photo') for n in range(43)]
        with patch('tg_forwarder.control.source.utils.get_peer_id',side_effect=lambda e:e.key):
            self.snap=await self.controller.resolver.resolve(SourceRef('tgfanchabiao',4739,23508))
        self.fresh=self.snap['main']+self.snap['discussion']
        self.assertEqual((len(self.fresh),sum(i['kind']=='photo' for i in self.fresh)),(53,46))
        old=copy.deepcopy(self.snap)
        for i in old['main']+old['discussion']:
            for k in ('folder_chat','folder_title','folder_topic_id','folder_topic_title','folder_collision','folder_month','folder_batch'):i['archive_meta'].pop(k,None)
            path=self.engine.archive.file(old,i);path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'data');path.chmod(0o600)
            key=digest([i['chat'],i['id'],i.get('media_id','')]);self.store.db.execute('INSERT INTO cache VALUES(?,?,?)',(key,str(path),4))
        payload={'action':'download','target':None,'snapshot':old,'items':old['main']+old['discussion']}
        self.original=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,payload)
        self.store.set_task(self.original['id'],state='done',progress=53)
        self.original=self.store.task(self.original['id'],inbound.OWNER,inbound.OWNER)
        self.first_ids=set(range(23508,23538))|{23543}
        self.first=[i for i in self.fresh if i['chat']==source.GROUP and i['id'] in self.first_ids]
        self.rest=[i for i in self.fresh if i not in self.first]
        self.assertEqual((len(self.first),len(self.rest),sum(i['kind']=='video' for i in self.rest)),(31,22,7))

    def path(self,i):
        return Path(self.store.db.execute('SELECT path FROM cache WHERE key=?',(digest([i['chat'],i['id'],i.get('media_id','')]),)).fetchone()[0])
    def delete(self,items):
        for i in items:self.path(i).unlink()
    async def submit(self):
        q=self.store.query(inbound.OWNER,inbound.OWNER,78001,self.snap)
        await self.controller.callback(inbound.OWNER,inbound.OWNER,78001,{'query':q,'scope':'all','do':'download','confirmed':True})
        head=self.store.get_meta('missing-repair-head:'+self.original['id'])
        return self.store.task(head,inbound.OWNER,inbound.OWNER)
    def available(self):return sum(self.path(i).is_file() and self.path(i).stat().st_size==4 for i in self.fresh)
    def calls(self):return sum(m.download_media.await_count for m in self.client.main+self.client.replies)

    async def test_one_submit31_missing_then22_deleted_after_reuse_finishes_full53(self):
        await self.fixture();before=dict(self.original);self.delete(self.first)
        task=await self.submit();p=json.loads(task['payload'])
        self.assertEqual(len(p['items']),53);self.assertTrue(p['missing_repair']['full_scope'])
        self.assertEqual(sum(e['existing'] is None for e in p['missing_repair']['entries']),31)
        deleted=False
        async def notice(current,final=False):
            nonlocal deleted
            if not final and not deleted and self.store.task(current['id'],inbound.OWNER,inbound.OWNER)['progress']==53:
                deleted=True;self.delete(self.rest)
        with patch.object(self.engine,'notice',side_effect=notice):await self.engine.execute(self.store.claim())
        done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(done['state'],'done',done['error']);self.assertEqual(done['progress'],53)
        self.assertEqual(self.available(),53);self.assertEqual(self.calls(),53)
        self.assertTrue(all(self.path(i).parent.parent.name=='反差婊投稿曝光' for i in self.fresh))
        self.assertEqual(json.loads(self.store.get_meta('task-last-run:'+task['id'])),{'attempt':1,'fresh':53,'reused':0})
        self.assertEqual(self.store.task(self.original['id'],inbound.OWNER,inbound.OWNER),before)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM operations').fetchone()[0],0)
        manifest=json.loads((self.engine.archive.metadata_dir/('manifest_'+task['id']+'.json')).read_text())
        self.assertEqual(manifest['coverage']['verified'],53);self.assertEqual(len(manifest['items']),53)
        await self.controller.render_task(inbound.OWNER,inbound.OWNER,78001,done)
        text=self.bot.edit_message_text.call_args.kwargs['text'];self.assertIn('完整範圍 53',text);self.assertIn('46相＋7片',text)

    async def test_stable_existing22_no_redownload_full53_counts31fresh22reuse(self):
        await self.fixture();self.delete(self.first);task=await self.submit();await self.engine.execute(self.store.claim())
        done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER);self.assertEqual(done['state'],'done',done['error'])
        self.assertEqual(self.available(),53);self.assertEqual(self.calls(),31)
        self.assertEqual(json.loads(self.store.get_meta('task-last-run:'+task['id']))['reused'],22)
        again=await self.submit();self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2);self.assertEqual(self.calls(),31)

    async def test_prior_done_subset31_chain_does_not_truncate_next_full_request(self):
        await self.fixture();self.delete(self.first)
        entries=[{'key':digest([i['chat'],i['id'],i.get('media_id','')]),'relative':str(self.engine.archive.file(self.snap,i).relative_to(self.config.root)),'previous_relative':str(self.path(i).relative_to(self.config.root)),'size':4} for i in self.first]
        payload={'action':'download','target':None,'snapshot':json.loads(self.original['payload'])['snapshot'],'items':self.first,'missing_repair':{'origin':self.original['id'],'revision':digest(self.original['payload']),'previous':'','explicit_resubmit':True,'entries':entries}}

        with patch('tg_forwarder.control.store.archive_batch',side_effect=lambda p,t,c:p):
            prior=self.store.create_task(inbound.OWNER,inbound.OWNER,78002,payload)
        self.store.meta('missing-repair-head:'+self.original['id'],prior['id']);await self.engine.execute(self.store.claim())
        prior=self.store.task(prior['id'],inbound.OWNER,inbound.OWNER);self.assertEqual(prior['state'],'done',prior['error'])
        receipts=[tuple(r) for r in self.store.db.execute('SELECT key,value FROM meta WHERE key LIKE ?',('repair-publication:'+prior['id']+':%',))]
        self.delete(self.rest);task=await self.submit();self.assertEqual(len(json.loads(task['payload'])['items']),53)
        await self.engine.execute(self.store.claim());done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(done['state'],'done',done['error']);self.assertEqual(self.available(),53);self.assertEqual(self.calls(),53)
        self.assertEqual(self.store.task(prior['id'],inbound.OWNER,inbound.OWNER),prior)
        self.assertEqual(receipts,[tuple(r) for r in self.store.db.execute('SELECT key,value FROM meta WHERE key LIKE ?',('repair-publication:'+prior['id']+':%',))])

    async def test_same_path_metadata_or_inode_drift_fail_closed_before_source(self):
        await self.fixture();self.delete(self.first);task=await self.submit()
        path=self.path(self.rest[0]);original=path.stat()
        for kind in ('mode','mtime','inode','bytes'):
            if kind=='mode':path.chmod(0o640)
            elif kind=='mtime':os.utime(path,ns=(original.st_atime_ns,original.st_mtime_ns+1000000))
            elif kind=='inode':
                path.rename(path.with_suffix('.retained'));path.write_bytes(b'data');path.chmod(0o600);os.utime(path,ns=(original.st_atime_ns,original.st_mtime_ns))
            else:path.write_bytes(b'evil');os.utime(path,ns=(original.st_atime_ns,original.st_mtime_ns))
            await self.engine.execute(self.store.claim());done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
            self.assertEqual(done['state'],'failed',kind);self.assertEqual(self.calls(),0)
            if kind=='inode':path.unlink();path.with_suffix('.retained').rename(path)
            else:path.write_bytes(b'data')
            path.chmod(0o600);os.utime(path,ns=(original.st_atime_ns,original.st_mtime_ns))
            if kind!='bytes':self.store.retry(task['id'],inbound.OWNER,inbound.OWNER)

    async def test_deletion_during_proof_inventory_still_one_full_round(self):
        await self.fixture();self.delete(self.first)
        from tg_forwarder.canonical_media import fingerprint
        deleted=False
        old_paths=[self.path(i) for i in self.rest]
        def racing(path):
            nonlocal deleted
            if not deleted:
                deleted=True
                for old_path in old_paths:old_path.unlink()
            return fingerprint(path)
        with patch('tg_forwarder.canonical_media.fingerprint',side_effect=racing):task=await self.submit()
        self.assertEqual(len(json.loads(task['payload'])['items']),53)
        await self.engine.execute(self.store.claim());done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(done['state'],'done',done['error']);self.assertEqual(self.available(),53);self.assertEqual(self.calls(),53)

    async def test_recovery_source_acl_failure_not_done_or_full_progress(self):
        await self.fixture();self.delete(self.first);task=await self.submit();deleted=False
        async def notice(current,final=False):
            nonlocal deleted
            if not final and not deleted and self.store.task(current['id'],inbound.OWNER,inbound.OWNER)['progress']==53:
                deleted=True;self.delete(self.rest)
                async def denied(*args,**kwargs):raise PermissionError('source ACL denied')
                self.client.get_messages=denied
        with patch.object(self.engine,'notice',side_effect=notice):await self.engine.execute(self.store.claim())
        done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(done['state'],'failed');self.assertEqual(done['progress'],31);self.assertEqual(self.available(),31)
        manifest=json.loads((self.engine.archive.metadata_dir/('manifest_'+task['id']+'.json')).read_text())
        self.assertEqual(manifest['coverage']['verified'],31);self.assertEqual(len(manifest['unfinished']),22)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM operations').fetchone()[0],0)

    async def test_earlier_file_deleted_during_final_scope_sweep_is_recovered(self):
        await self.fixture();self.delete(self.first);task=await self.submit()
        original=self.engine.repair_destination;deleted=False
        async def checking(current,payload,selected,require_existing=False,**kwargs):
            nonlocal deleted
            path=await original(current,payload,selected,require_existing,**kwargs)
            if require_existing and not deleted and selected==payload['items'][-1] and self.store.task(current['id'],inbound.OWNER,inbound.OWNER)['progress']==53:
                deleted=True;self.path(self.rest[0]).unlink()
            return path
        with patch.object(self.engine,'repair_destination',side_effect=checking):await self.engine.execute(self.store.claim())
        done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(done['state'],'done',done['error']);self.assertTrue(deleted);self.assertEqual(self.available(),53);self.assertEqual(self.calls(),32)

    async def test_late_same_inode_size_corruption_rejected_even_restored_mtime(self):
        await self.fixture();self.delete(self.first);task=await self.submit()
        original=self.engine.repair_destination;changed=False;victim=None
        async def checking(current,payload,selected,require_existing=False,**kwargs):
            nonlocal changed,victim
            path=await original(current,payload,selected,require_existing,**kwargs)
            if kwargs.get('observed') is not None and not changed and selected==payload['items'][-1]:
                changed=True;victim=self.path(self.first[0]);before=victim.stat()
                victim.write_bytes(b'evil');os.utime(victim,ns=(before.st_atime_ns,before.st_mtime_ns))
                self.assertEqual(victim.stat().st_ino,before.st_ino);self.assertEqual(victim.stat().st_size,before.st_size)
            return path
        with patch.object(self.engine,'repair_destination',side_effect=checking):await self.engine.execute(self.store.claim())
        done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertTrue(changed);self.assertEqual(done['state'],'failed');self.assertIn('verified file attributes changed',done['error'])
        self.assertEqual(victim.read_bytes(),b'evil');self.assertEqual(self.calls(),31)
        self.assertEqual(self.store.get_meta('missing-repair-head:'+self.original['id']),task['id'])

    async def test_continuing_deletion_exhausts_bound_never_false_done(self):
        await self.fixture();self.delete(self.first);task=await self.submit()
        async def notice(current,final=False):
            if not final and self.store.task(current['id'],inbound.OWNER,inbound.OWNER)['progress']==53:
                for i in self.fresh:
                    try:self.path(i).unlink()
                    except FileNotFoundError:pass
        with patch.object(self.engine,'notice',side_effect=notice):await self.engine.execute(self.store.claim())
        done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER);self.assertEqual(done['state'],'failed');self.assertIn('not DONE',done['error'])
        self.assertLessEqual(self.calls(),31+2*53);self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)

    async def test_only_one_missing_of53_still_allocates_fullscope_batch(self):
        await self.fixture();retained={digest([i['chat'],i['id'],i.get('media_id','')]):self.path(i) for i in self.rest+self.first[1:]}
        self.delete(self.first[:1]);task=await self.submit();payload=json.loads(task['payload'])
        self.assertEqual(payload['archive_batch']['logical_count'],53)
        await self.engine.execute(self.store.claim());done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(done['state'],'done',done['error']);self.assertEqual(self.calls(),1)
        self.assertEqual(self.path(self.first[0]).parent.name,payload['archive_batch']['folder'])
        for i in self.rest+self.first[1:]:self.assertEqual(self.path(i),retained[digest([i['chat'],i['id'],i.get('media_id','')])])
