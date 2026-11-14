"""Native task allocation -> filesystem, no live Telegram calls."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import test_source_archive_group as grouping
import test_inbound_photos as inbound
from tg_forwarder.canonical_media import message_month
from tg_forwarder.control.store import archive_batch,Store


class BatchTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=grouping.MainArchiveTests.asyncSetUp
    asyncTearDown=grouping.MainArchiveTests.asyncTearDown
    transport=grouping.MainArchiveTests.transport
    forbid_source=grouping.MainArchiveTests.forbid_source
    source=grouping.MainArchiveTests.source
    resolve=grouping.MainArchiveTests.resolve

    async def payload(self,n):
        self.source();snap=await self.resolve()
        items=(snap['main']+snap['discussion'])[:n]
        for pos,i in enumerate(items):i['archive_meta']['datetime']='2020-01-31T23:59:59+00:00' if pos%2 else '2021-12-01T00:00:00+00:00'
        return {'snapshot':snap,'items':items,'action':'download','target':None}

    async def test_cardinality_zero_one_two53_and_duplicate_identity(self):
        for n in (0,1,2,53):
            p=await self.payload(n);frozen=copy.deepcopy(p)
            allocated=archive_batch(p,'abcdef1200000001',1794673440)
            self.assertEqual(p,frozen)
            self.assertEqual('archive_batch' in allocated,n>1)
            if n>1:
                self.assertEqual(allocated['archive_batch']['logical_count'],n)
                self.assertRegex(allocated['archive_batch']['folder'],r'^20261115_002400-abcdef12$')
                paths=[self.engine.archive.file(p['snapshot'],i) for i in allocated['items']]
                self.assertEqual(len({v.parent for v in paths}),1)
                self.assertEqual(paths[0].parent.parent,self.config.root/'反差婊投稿曝光')
            elif n==1:self.assertEqual(self.engine.archive.file(p['snapshot'],allocated['items'][0]).parent.name,'2021_12')
        p=await self.payload(1);p['items']*=2
        self.assertNotIn('archive_batch',archive_batch(p,'abcdef1200000001',1794673440))

    async def test_native_different_months_one_batch_and_restart_retry_same_allocation(self):
        p=await self.payload(2)
        task=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,p)
        persisted=json.loads(task['payload']);name=persisted['archive_batch']['folder']
        self.assertEqual(self.store.create_task(inbound.OWNER,inbound.OWNER,78002,p)['id'],task['id'])
        # First acquisition fails; retry must not allocate another directory.
        original=self.client.main[0].download_media.side_effect
        self.client.main[0].download_media.side_effect=RuntimeError('offline')
        await self.engine.execute(self.store.claim());self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER)['state'],'delayed')
        self.store.set_task(task['id'],state='failed')
        self.client.main[0].download_media.side_effect=original
        self.store.retry(task['id'],inbound.OWNER,inbound.OWNER)
        await self.engine.execute(self.store.claim())
        done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER);self.assertEqual(done['state'],'done',done['error'])
        paths=[Path(r[0]) for r in self.store.db.execute('SELECT path FROM cache')]
        self.assertEqual(len(paths),2);self.assertTrue(all(v.parent.name==name for v in paths))
        db=Path(self.store.db.execute('PRAGMA database_list').fetchone()[2]);other=Store(db)
        try:self.assertEqual(other.task(task['id'],inbound.OWNER,inbound.OWNER)['payload'],task['payload'])
        finally:other.close()
        self.assertEqual(len(list((self.config.root/'反差婊投稿曝光').iterdir())),1)

    async def test_foreign_batch_directory_never_adopted_or_overwritten(self):
        p=await self.payload(2);task=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,p)
        p=json.loads(task['payload']);path=self.engine.archive.file(p['snapshot'],p['items'][0])
        path.parent.mkdir(parents=True);sentinel=path.parent/'foreign';sentinel.write_bytes(b'keep')
        await self.engine.execute(self.store.claim())
        done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER);self.assertEqual(done['state'],'failed')
        self.assertEqual(sentinel.read_bytes(),b'keep');self.client.main[0].download_media.assert_not_awaited()

    async def test_single_original_instant_timezone_boundary_not_current_clock(self):
        p=await self.payload(1);i=p['items'][0];i['archive_meta']['datetime']='2020-01-31T16:00:00+00:00'
        self.assertEqual(message_month(i,'Asia/Hong_Kong'),'2020_02');self.assertEqual(message_month(i,'UTC'),'2020_01')
        for bad in (None,'','nonsense','2020-01-01T00:00:00'):
            i['archive_meta']['datetime']=bad
            with self.assertRaises(ValueError):message_month(i)

    async def test_distinct_same_second_batches_have_stable_distinct_suffixes(self):
        p=await self.payload(2)
        a=archive_batch(p,'abcdef1200000001',1794673440)
        b=archive_batch(p,'fedcba9800000002',1794673440)
        self.assertNotEqual(a['archive_batch']['folder'],b['archive_batch']['folder'])
        self.assertEqual(a,archive_batch(p,'abcdef1200000001',1794673440))

    async def test_native_single_comment_original_month_and_missing_timestamp_fail_closed(self):
        from datetime import datetime,timezone
        from tg_forwarder.control.input import SourceRef
        self.source();msg=self.client.replies[0]
        msg.date=datetime(2020,1,31,16,tzinfo=timezone.utc)
        msg.forward_date=datetime(2026,11,15,tzinfo=timezone.utc)
        with patch('tg_forwarder.control.source.utils.get_peer_id',side_effect=lambda e:e.key):
            snap=await self.resolve(msg.id)
        selected=next(i for i in snap['discussion'] if i['id']==msg.id)
        task=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,{'action':'download','target':None,'snapshot':snap,'items':[selected]})
        self.assertNotIn('archive_batch',json.loads(task['payload']))
        await self.engine.execute(self.store.claim());done=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(done['state'],'done',done['error'])
        path=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0])
        self.assertEqual(path.parent.name,'2020_01');self.assertEqual(path.parent.parent.name,'反差婊投稿曝光')
        self.assertEqual(msg.download_media.await_count,1)
        msg.date=None
        with self.assertRaises(grouping.Unavailable):await self.resolve(msg.id)

    async def test_native_standalone_single_deleted_resubmit_keeps_original_month(self):
        from datetime import datetime,timezone
        from unittest.mock import AsyncMock
        from tg_forwarder.control.input import SourceRef
        import test_media_workbench as media
        self.source();seed=self.client.replies[0];seed.grouped_id=None
        seed.date=datetime(2019,12,31,23,59,tzinfo=timezone.utc)
        self.client.get_messages=AsyncMock(return_value=seed)
        async def resolve():
            with patch('tg_forwarder.control.source.utils.get_peer_id',side_effect=lambda e:e.key):
                return await self.controller.resolver.resolve(SourceRef(media.GROUP,seed.id))
        snap=await resolve();items=snap['main']+snap['discussion'];self.assertEqual(len(items),1)
        task=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,{'action':'download','target':None,'snapshot':snap,'items':items})
        await self.engine.execute(self.store.claim());original=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(original['state'],'done',original['error'])
        path=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0]);self.assertEqual(path.parent.name,'2019_12')
        path.unlink()
        with patch('tg_forwarder.control.store.time.time',return_value=1794673440):
            q=self.store.query(inbound.OWNER,inbound.OWNER,78001,await resolve())
            await self.controller.callback(inbound.OWNER,inbound.OWNER,78001,{'query':q,'scope':'single','do':'download','confirmed':True})
        head=self.store.get_meta('missing-repair-head:'+task['id']);pending=self.store.claim()
        self.assertEqual(pending['id'],head);self.assertNotIn('archive_batch',json.loads(pending['payload']))
        await self.engine.execute(pending);done=self.store.task(head,inbound.OWNER,inbound.OWNER)
        self.assertEqual(done['state'],'done',done['error'])
        self.assertEqual(Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0]),path)
        self.assertEqual(path.read_bytes(),b'data');self.assertEqual(seed.download_media.await_count,2)
        self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER),original)
