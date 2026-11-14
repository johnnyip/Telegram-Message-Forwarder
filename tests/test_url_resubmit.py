"""Explicit query download action only: no real media or production services."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock,patch
import test_download_locations as locations
import test_inbound_photos as inbound
from tg_forwarder.control.store import encode,digest


class ResubmitTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=locations.DownloadLocationTests.asyncSetUp
    asyncTearDown=locations.DownloadLocationTests.asyncTearDown
    fixture=locations.DownloadLocationTests.fixture
    transport=locations.DownloadLocationTests.transport
    forbid_source=locations.DownloadLocationTests.forbid_source

    def setup_original(self,count=53,action='download'):
        task=self.fixture(count);original=json.loads(task['payload'])
        if action=='both':
            self.store.db.execute('DELETE FROM tasks WHERE id=?',(task['id'],))
            original['action']='both';original['target']=[inbound.OWNER,None]
            task=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,original)
            self.store.set_task(task['id'],state='done',progress=count)
            task=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.store.meta('bot_principal',str(inbound.BOT))
        snap=json.loads(task['payload'])['snapshot']
        fresh=[]
        for n,i in enumerate(original['items']):
            meta={'title':'反差婊投稿曝光' if n<10 else '评论区','folder_title':'反差婊投稿曝光' if n<10 else '评论区','filename':str(i['id'])+'.jpg' if i['kind']=='photo' else str(i['id'])+'.mp4'}
            fresh.append({**i,'archive_meta':meta})
        snap={**snap,'selected':fresh[:1],'selected_album':fresh[:1],'main':fresh[:10],'discussion':fresh[10:],'partial':False,'unsupported':[]}
        return task,{'snapshot':snap,'items':fresh,'action':action,'target':original.get('target')}

    async def submit(self,requested):
        q=self.store.query(inbound.OWNER,inbound.OWNER,78001,requested['snapshot'])
        await self.controller.callback(inbound.OWNER,inbound.OWNER,78001,{'query':q,'scope':'single' if len(requested['items'])==1 else 'all','do':requested['action'],'target':requested.get('target'),'confirmed':True})
        head=self.store.get_meta('missing-repair-head:'+self.original['id'],'')
        return self.store.task(head,inbound.OWNER,inbound.OWNER) if head else self.original

    def delete_all(self):
        for row in self.store.db.execute('SELECT path FROM cache'):Path(row['path']).unlink()

    async def run_round(self):
        task=self.store.claim();payload=json.loads(task['payload'])
        selected={i['id']:i for i in payload['items']}
        async def download(file,progress_callback):
            file.write(b'data');await progress_callback(4,4);return file
        async def get_message(peer,ids):return NS(media=object(),selected=selected[ids],download_media=AsyncMock(side_effect=download))
        self.client.get_messages=AsyncMock(side_effect=get_message)
        self.client.get_entity=AsyncMock(side_effect=AssertionError('Confirmed metadata retained; no entity fallback'))
        with patch('tg_forwarder.control.source.item',side_effect=lambda msg,section:msg.selected):await self.engine.execute(task)
        return self.store.task(task['id'],inbound.OWNER,inbound.OWNER)

    async def test_actual53_deleted_fresh_query_new_round_canonical10_43_repeat_delete(self):
        self.original,requested=self.setup_original();old=dict(self.original);self.delete_all()
        first=await self.submit(requested);self.assertNotEqual(first['id'],self.original['id'])
        again=await self.submit(requested);self.assertEqual(first['id'],again['id'])
        done=await self.run_round();self.assertEqual(done['state'],'done',done['error'])
        self.assertEqual(self.client.get_messages.await_count,53)
        manifest=json.loads((self.engine.archive.metadata_dir/('manifest_'+done['id']+'.json')).read_text())
        self.assertEqual(len(manifest['items']),53)
        self.assertTrue(all(not Path(i['path']).is_absolute() and not i['path'].startswith('workbench/') for i in manifest['items']))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM meta WHERE key LIKE ?',('repair-publication:'+done['id']+':%',)).fetchone()[0],53)
        paths=[Path(row['path']) for row in self.store.db.execute('SELECT path FROM cache')]
        self.assertEqual([sum(p.parent.name==folder for p in paths) for folder in ('反差婊投稿曝光','评论区')],[10,43])
        self.assertTrue(all(p.read_bytes()==b'data' for p in paths));self.assertFalse(any('workbench' in str(p.relative_to(self.config.root)) for p in paths))
        self.assertEqual(self.store.task(self.original['id'],inbound.OWNER,inbound.OWNER),old)
        self.assertEqual(json.loads(self.store.get_meta('task-last-run:'+done['id'])),{'attempt':1,'fresh':53,'reused':0})
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM operations').fetchone()[0],0)
        await self.submit(requested);self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)
        self.delete_all();second=await self.submit(requested);self.assertNotEqual(second['id'],first['id'])
        done=await self.run_round();self.assertEqual(done['state'],'done',done['error'])
        self.bot.get_file.assert_not_awaited()

    async def test_both_round_download_only_preserves_operations(self):
        self.original,requested=self.setup_original(2,'both');self.delete_all()
        self.store.mark('prior-forward','done',123)
        before=[tuple(r) for r in self.store.db.execute('SELECT * FROM operations')]
        derived=await self.submit(requested);p=json.loads(derived['payload'])
        self.assertEqual(p['action'],'download');self.assertIsNone(p['target'])
        done=await self.run_round();self.assertEqual(done['state'],'done',done['error'])
        self.assertEqual(before,[tuple(r) for r in self.store.db.execute('SELECT * FROM operations')])

    async def test_present_no_round_render_no_repair(self):
        self.original,requested=self.setup_original(2)
        await self.submit(requested);self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],1)
        self.delete_all();await self.controller.render_task(inbound.OWNER,inbound.OWNER,78001,self.original)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],1)
        self.assertIn('遺失 2',self.bot.edit_message_text.call_args.kwargs['text'])

    async def test_failed_cancelled_ambiguous_heads_use_native_controls(self):
        self.original,requested=self.setup_original(2);self.delete_all();first=await self.submit(requested)
        for state in ('failed','cancelled','ambiguous','running','delayed'):
            self.store.set_task(first['id'],state=state);same=await self.submit(requested)
            self.assertEqual(same['id'],first['id']);self.assertEqual(same['state'],state)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)

    async def test_foreign_destination_and_symlink_reject_no_source(self):
        self.original,requested=self.setup_original(2);self.delete_all()
        path=self.engine.archive.file({},requested['items'][-1]);path.parent.mkdir(parents=True);path.write_bytes(b'evil')
        with self.assertRaises(ValueError):await self.submit(requested)
        self.assertEqual(path.read_bytes(),b'evil');path.unlink();path.symlink_to(self.config.root/'absent')
        with self.assertRaises(ValueError):await self.submit(requested)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],1)

    async def test_raced_destination_after_queue_fails_before_source(self):
        self.original,requested=self.setup_original(1);self.delete_all();derived=await self.submit(requested)
        path=self.engine.archive.file({},requested['items'][0]);path.parent.mkdir(parents=True);path.write_bytes(b'evil');path.chmod(0o600)
        self.client.get_messages=AsyncMock(side_effect=AssertionError('No source'))
        task=self.store.claim();await self.engine.execute(task)
        result=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(result['state'],'failed');self.assertIn('occupied',result['error']);self.client.get_messages.assert_not_awaited()
        self.assertEqual(path.read_bytes(),b'evil')

    async def test_source_identity_changed_fails_no_download(self):
        self.original,requested=self.setup_original(1);self.delete_all();await self.submit(requested)
        message=NS(media=object(),download_media=AsyncMock(side_effect=AssertionError('No bytes')))
        self.client.get_messages=AsyncMock(return_value=message)
        with patch('tg_forwarder.control.source.item',return_value={**requested['items'][0],'media_id':'changed'}):
            task=self.store.claim();await self.engine.execute(task)
        self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER)['state'],'failed');message.download_media.assert_not_awaited()

    async def test_concurrent_submissions_one_round(self):
        self.original,requested=self.setup_original(2);self.delete_all()
        first,second=await asyncio.gather(self.submit(requested),self.submit(requested))
        self.assertEqual(first['id'],second['id']);self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)

    async def test_old_path_race_and_permission_denial_fail_closed(self):
        self.original,requested=self.setup_original(1)
        old=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0]);self.delete_all();await self.submit(requested)
        old.write_bytes(b'evil');old.chmod(0o600)
        self.client.get_messages=AsyncMock(side_effect=AssertionError('No source'))
        task=self.store.claim();await self.engine.execute(task)
        result=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(result['state'],'failed');self.assertIn('Previously missing',result['error']);self.client.get_messages.assert_not_awaited()
        old.unlink();self.store.retry(task['id'],inbound.OWNER,inbound.OWNER)
        self.client.get_messages=AsyncMock(side_effect=PermissionError('source ACL denied'))
        task=self.store.claim();await self.engine.execute(task)
        self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER)['state'],'failed')
        self.assertFalse(self.engine.archive.file({},requested['items'][0]).exists())

    async def test_folder_identity_drift_before_execution_rejected(self):
        self.original,requested=self.setup_original(1);self.delete_all();await self.submit(requested)
        self.store.meta('archive_folder:'+digest('反差婊投稿曝光'),encode([999,None]))
        self.client.get_messages=AsyncMock(side_effect=AssertionError('No source'))
        task=self.store.claim();await self.engine.execute(task)
        result=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(result['state'],'failed');self.assertIn('folder identity collision',result['error']);self.client.get_messages.assert_not_awaited()

    async def test_confirmed_topic_native_folder(self):
        self.original,requested=self.setup_original(1);self.delete_all()
        requested['items'][0]['archive_meta'].update(topic_id=17,topic_title='相片主題')
        await self.submit(requested);done=await self.run_round()
        self.assertEqual(done['state'],'done',done['error'])
        path=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0])
        self.assertEqual(path.parent.name,'反差婊投稿曝光 - 相片主題')

    async def test_held_lock_target_race_rejected_before_source(self):
        self.original,requested=self.setup_original(1);self.delete_all();await self.submit(requested)
        task=self.store.claim();path=self.engine.archive.file({},requested['items'][0])
        self.client.get_messages=AsyncMock(side_effect=AssertionError('No source'))
        await self.engine.download_lock.acquire();job=asyncio.create_task(self.engine.execute(task))
        try:
            for _ in range(1000):
                if self.engine.download_lock._waiters:break
                await asyncio.sleep(0.001)
            self.assertTrue(self.engine.download_lock._waiters)
            path.parent.mkdir(parents=True);path.write_bytes(b'evil');path.chmod(0o600)
        finally:self.engine.download_lock.release()
        await job;self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER)['state'],'failed')
        self.assertEqual(path.read_bytes(),b'evil');self.client.get_messages.assert_not_awaited()

    async def test_wrong_principal_and_missing_metadata_fail_closed(self):
        self.original,requested=self.setup_original(1);self.delete_all();self.store.meta('bot_principal','wrong')
        with self.assertRaises(PermissionError):await self.submit(requested)
        self.store.meta('bot_principal',str(inbound.BOT));requested['items'][0].pop('archive_meta')
        # snapshot holds same selected dictionaries, as a real resolver would.
        with self.assertRaises(ValueError):await self.controller.resubmit_download(inbound.OWNER,inbound.OWNER,78001,self.original,requested)
        requested['items'][0]['archive_meta']={'title':str(requested['items'][0]['chat'])}
        with self.assertRaises(ValueError):await self.controller.resubmit_download(inbound.OWNER,inbound.OWNER,78001,self.original,requested)
