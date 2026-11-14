"""Inbound physical alias reuse only from private proven cache bindings, offline."""
import copy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import test_flat_archive as flat
import test_inbound_photos as inbound
from tg_forwarder.control.engine import Engine, RateGate
from tg_forwarder.control.store import Store, digest


class PhotoAliasTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=flat.FlatTests.asyncSetUp
    asyncTearDown=flat.FlatTests.asyncTearDown
    execute=flat.FlatTests.execute
    forbid_source=flat.FlatTests.forbid_source
    assert_no_source=flat.FlatTests.assert_no_source
    transport=flat.FlatTests.transport
    queue=flat.FlatTests.queue

    def raw(self,mid,unique='same-photo-variant',size=4):
        raw=inbound.raw_update(mid)
        raw['message']['photo'][-1].update(file_unique_id=unique,file_size=size)
        return raw

    async def receive(self,mid,unique='same-photo-variant',size=4):
        await self.queue(self.raw(mid,unique,size));return await self.execute()

    async def first(self):
        self.transport();row=await self.receive(80001)
        self.assertEqual(row['state'],'done',row['error'])
        payload=json.loads(row['payload']);item=payload['items'][0]
        key=digest([item['chat'],item['id'],item['media_id']])
        path=Path(self.store.db.execute('SELECT path FROM cache WHERE key=?',(key,)).fetchone()[0])
        return row,payload,item,path

    async def test_two_message_ids_same_physical_photo_one_get_file_and_two_cache_refs(self):
        await self.first();row=await self.receive(80002)
        self.assertEqual(row['state'],'done',row['error'])
        rows=self.store.db.execute('SELECT path FROM cache').fetchall()
        self.assertEqual(len(rows),2);self.assertEqual(len({r[0] for r in rows}),1)
        self.assertEqual(self.bot.get_file.await_count,1);self.assertEqual(len(list(self.config.root.glob('*.jpg'))),1)
        self.assert_no_source();self.bot.send_photo.assert_not_awaited()
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)

    async def test_alias_survives_real_store_close_and_engine_restart(self):
        await self.first();self.store.close();self.store=Store(self.config.db)
        self.gate=RateGate(self.store)
        self.engine=Engine(self.config,self.store,self.client,self.gate,delivery=self.engine.delivery,bot_id=inbound.BOT)
        self.controller.store=self.store;self.controller.engine=self.engine
        row=await self.receive(80002)
        self.assertEqual(row['state'],'done',row['error']);self.assertEqual(self.bot.get_file.await_count,1)

    async def test_strict_owner_bot_media_and_decoded_file_binding_before_alias(self):
        row,payload,item,path=await self.first()
        variants=[]
        other=copy.deepcopy(item);other['bot_id']+=1;variants.append((dict(row),payload,other))
        other=copy.deepcopy(item);other['media_id']='bot-inbound:other';variants.append((dict(row),payload,other))
        other=copy.deepcopy(item);other['file_id']='unsupported-wrong-photo';variants.append((dict(row),payload,other))
        owner=dict(row);owner['owner']=192114587;variants.append((owner,payload,item))
        for task,p,selected in variants:
            with self.assertRaises(PermissionError):await self.engine.download(task,p,selected,0)
        self.assertEqual(self.bot.get_file.await_count,1);self.assertEqual(path.read_bytes(),b'data')

    async def test_different_unique_variant_never_reuses_same_photo_id_or_same_size(self):
        await self.first();row=await self.receive(80002,unique='different-variant')
        self.assertEqual(row['state'],'done',row['error']);self.assertEqual(self.bot.get_file.await_count,2)
        self.assertEqual(len({r[0] for r in self.store.db.execute('SELECT path FROM cache')}),2)

    async def test_different_dimensions_never_reuses_same_unique_and_size(self):
        await self.first();raw=self.raw(80002)
        raw['message']['photo'][-1].update(width=1920,height=1080)
        await self.queue(raw);row=await self.execute()
        self.assertEqual(row['state'],'done',row['error']);self.assertEqual(self.bot.get_file.await_count,2)

    async def test_different_size_does_not_hit_alias(self):
        await self.first();row=await self.receive(80002,size=5)
        self.assertNotEqual(row['state'],'done');self.assertEqual(self.bot.get_file.await_count,2)

    async def test_known_alias_corrupt_truncated_or_group_writable_fails_without_transport(self):
        row,payload,item,path=await self.first()
        original=path.read_bytes()
        for mid,content,mode in ((80002,b'evil',0o600),(80003,b'x',0o600),(80004,original,0o666)):
            path.chmod(0o600);path.write_bytes(content);path.chmod(mode)
            row=await self.receive(mid)
            self.assertNotEqual(row['state'],'done',str(mid))
            self.assertEqual(self.bot.get_file.await_count,1)
        path.chmod(0o600)

    async def test_readable_foreign_owner_alias_rejected_without_transport(self):
        _,_,_,path=await self.first();original=Path.lstat
        def foreign(p,*a,**kw):
            info=original(p,*a,**kw)
            if p==path:
                values=list(info);values[4]=999999;return os.stat_result(values)
            return info
        with patch.object(Path,'lstat',foreign),patch('tg_forwarder.canonical_media.trusted_file_owner_uids',return_value=frozenset()):
            row=await self.receive(80002)
        self.assertNotEqual(row['state'],'done');self.assertEqual(self.bot.get_file.await_count,1)

    async def test_missing_indexed_alias_reference_allows_current_fresh_download(self):
        _,_,item,_=await self.first()
        self.store.db.execute('DELETE FROM cache')
        row=await self.receive(80002)
        self.assertEqual(row['state'],'done',row['error']);self.assertEqual(self.bot.get_file.await_count,2)

    async def test_bounded_old_task_bootstrap_reuses_existing_known_file_no_rpc(self):
        old,payload,_,path=await self.first()
        payload['items'][0].pop('photo_variant')
        self.store.db.execute('UPDATE tasks SET payload=? WHERE id=?',(json.dumps(payload),old['id']))
        self.store.db.execute("DELETE FROM meta WHERE key LIKE 'flat_photo_alias:%'")
        row=await self.receive(80002)
        self.assertEqual(row['state'],'done',row['error']);self.assertEqual(self.bot.get_file.await_count,1)
        self.assertEqual({r[0] for r in self.store.db.execute('SELECT path FROM cache')},{str(path)})

    async def test_named_fresh_request_downloads_current_copy_without_moving_old_root_file(self):
        _,_,_,path=await self.first();self.engine.archive.named=True
        row=await self.receive(80002)
        self.assertEqual(row['state'],'done',row['error']);self.assertEqual(self.bot.get_file.await_count,2)
        self.assertEqual(path.read_bytes(),b'data');self.assertEqual(len(list(self.config.root.glob('*.jpg'))),1)
        self.assertEqual(len(list((self.config.root/'owner').glob('*.jpg'))),1)

    async def test_different_named_source_classification_does_not_alias_across_folders(self):
        self.engine.archive.named=True
        await self.first()
        raw=self.raw(80002);raw['message']['forward_origin']={'type':'channel','date':100,'chat':{'id':-1000000000010,'type':'channel','title':'Other channel'},'message_id':55}
        await self.queue(raw);row=await self.execute()
        self.assertEqual(row['state'],'done',row['error']);self.assertEqual(self.bot.get_file.await_count,2)
        parents={Path(r[0]).parent.name for r in self.store.db.execute('SELECT path FROM cache')}
        self.assertEqual(parents,{'owner','Other channel'})

    async def test_missing_old_file_fresh_copy_rebinds_alias_preserves_old_cache_then_reuses(self):
        _,_,old,path=await self.first()
        oldkey=digest([old['chat'],old['id'],old['media_id']])
        before=tuple(self.store.db.execute('SELECT * FROM cache WHERE key=?',(oldkey,)).fetchone())
        path.unlink()
        row=await self.receive(80002);self.assertEqual(row['state'],'done',row['error'])
        self.assertEqual(self.bot.get_file.await_count,2)
        self.assertEqual(tuple(self.store.db.execute('SELECT * FROM cache WHERE key=?',(oldkey,)).fetchone()),before)
        row=await self.receive(80003);self.assertEqual(row['state'],'done',row['error'])
        self.assertEqual(self.bot.get_file.await_count,2)
        paths=[r[0] for r in self.store.db.execute('SELECT path FROM cache WHERE key!=?',(oldkey,))]
        self.assertEqual(len(set(paths)),1);self.assertFalse(path.exists())
        with self.assertRaises(ValueError):await self.engine.cached_file_async(old)
        self.assertEqual(self.bot.get_file.await_count,2)

    async def test_stale_alias_cas_refuses_concurrent_change_and_rolls_back_new_message(self):
        row,payload,item,path=await self.first()
        identity=self.engine.photo_alias_identity(row,payload,item)
        name='flat_photo_alias:'+digest(identity);observed=self.store.get_meta(name)
        changed=json.loads(observed);changed['proof']['sha256']='concurrent-change'
        self.store.meta(name,json.dumps(changed))
        with self.assertRaisesRegex(ValueError,'changed concurrently'):
            self.engine.commit_photo_cache('new-message',path,4,identity,json.loads(observed),stale_alias=observed)
        self.assertIsNone(self.store.db.execute("SELECT * FROM cache WHERE key='new-message'").fetchone())
        self.assertEqual(json.loads(self.store.get_meta(name)),changed)

    async def test_missing_bootstrap_candidate_fetches_only_current_photo(self):
        _,_,old,path=await self.first()
        self.store.db.execute("DELETE FROM meta WHERE key LIKE 'flat_photo_alias:%'")
        path.unlink();row=await self.receive(80002)
        self.assertEqual(row['state'],'done',row['error']);self.assertEqual(self.bot.get_file.await_count,2)
        self.assert_no_source()

    async def test_same_size_corrupt_root_flat_proof_blocks_named_fresh_download(self):
        _,_,_,path=await self.first();path.write_bytes(b'evil')
        self.engine.archive.named=True
        row=await self.receive(80002)
        self.assertEqual(row['state'],'failed');self.assertEqual(self.bot.get_file.await_count,1)
        self.assertEqual(path.read_bytes(),b'evil')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM cache').fetchone()[0],1)
        self.assertFalse((self.config.root/'owner').exists())

    async def test_cache_and_alias_commit_are_atomic_on_binding_conflict(self):
        row,payload,item,path=await self.first()
        identity=self.engine.photo_alias_identity(row,payload,item)
        before=self.store.db.execute('SELECT COUNT(*) FROM cache').fetchone()[0]
        with self.assertRaises(ValueError):
            self.engine.commit_photo_cache('new-message',self.config.root/'unrelated.jpg',4,identity,{'proof':{'sha256':'wrong'}})
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM cache').fetchone()[0],before)
