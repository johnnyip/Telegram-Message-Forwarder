import asyncio
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import AsyncMock,patch
import test_named_albums as named
import test_inbound_photos as inbound
from tg_forwarder.control.store import digest,encode


class DownloadLocationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=named.NamedAlbumTests.asyncSetUp
    asyncTearDown=named.NamedAlbumTests.asyncTearDown
    forbid_source=named.NamedAlbumTests.forbid_source
    transport=named.NamedAlbumTests.transport

    def fixture(self,count=53,reply=78001):
        items=[]
        for index in range(count):
            section='main' if index<10 else 'discussion'
            folder='album-main' if index<10 else 'album-'+str((index-10)//10)
            relative=Path('workbench')/'-1004433596337'/'2026-11-10_4739'/section/folder/('相片 <safe> '+str(index)+('.jpg' if index<46 else '.mp4'))
            path=self.config.root/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'data');path.chmod(0o600)
            item={'chat':-1004433596337,'id':1000+index,'media_id':'document:'+str(index),'kind':'photo' if index<46 else 'video','size':4,'album':folder,'section':section,'caption':'','protected':False}
            items.append(item)
            self.store.db.execute('INSERT OR REPLACE INTO cache VALUES(?,?,?)',(digest([item['chat'],item['id'],item['media_id']]),str(path),4))
        payload={'action':'download','target':None,'snapshot':{'peer':-1004433596337,'post':4739,'main':items[:10],'discussion':items[10:]},'items':items}
        task=self.store.create_task(inbound.OWNER,inbound.OWNER,reply,payload)
        self.store.set_task(task['id'],state='done',progress=count)
        return self.store.task(task['id'],inbound.OWNER,inbound.OWNER)

    def data(self,task,page=0):return {'kind':'task','task':task['id'],'do':'locations','revision':digest(task['payload']),'bot':inbound.BOT,'page':page}

    async def test_53_legacy_nested_paths_paginate_same_card_metadata_only(self):
        task=self.fixture();before=self.bot.send_message.await_count
        with patch.object(self.engine,'cached_file_async',side_effect=AssertionError('No content-proof/hash route')):
            for page in range(6):
                await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],self.data(task,page))
                args=self.bot.edit_message_text.call_args.kwargs
                self.assertEqual(args['message_id'],task['reply']);self.assertIsNone(args['parse_mode']);self.assertLess(len(args['text']),4096)
                self.assertIn('53',args['text']);self.assertIn('可用 53/53',args['text']);self.assertIn('workbench/-1004433596337/2026-11-10_4739/',args['text'])
                for row in args['reply_markup'].inline_keyboard:
                    for button in row:self.assertLessEqual(len(button.callback_data.encode()),64)
        self.assertIn('第 6/6 頁',self.bot.edit_message_text.call_args.kwargs['text'])
        self.assertIn('相片 <safe>',self.bot.edit_message_text.call_args.kwargs['text'])
        self.assertEqual(self.bot.send_message.await_count,before);self.bot.get_file.assert_not_awaited()

    async def test_missing_wrong_size_symlink_and_outside_paths_honest_no_leaks(self):
        task=self.fixture(5);rows=self.store.db.execute('SELECT * FROM cache ORDER BY rowid').fetchall()
        Path(rows[0]['path']).unlink();Path(rows[1]['path']).write_bytes(b'bad')
        target=Path(rows[2]['path']);target.unlink();target.symlink_to(Path(rows[3]['path']))
        self.store.db.execute('UPDATE cache SET path=? WHERE key=?',('/private-secret-outside-root/token.txt',rows[4]['key']))
        await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],self.data(task))
        text=self.bot.edit_message_text.call_args.kwargs['text']
        self.assertIn('可用 1/5',text);self.assertIn('檔案遺失 1',text);self.assertIn('不安全',text)
        self.assertNotIn('private-secret',text);self.assertNotIn('token.txt',text);self.bot.get_file.assert_not_awaited()

    async def test_owner_bot_revision_and_page_guards(self):
        task=self.fixture(1);data=self.data(task)
        for field,value in (('bot',inbound.BOT+1),('revision','0'*64),('page',999),('page',-1),('page',True)):
            changed={**data,field:value}
            with self.assertRaises((ValueError,PermissionError)):await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],changed)
        with self.assertRaises((PermissionError,ValueError)):await self.controller.task_callback(192114587,192114587,task['reply'],data)
        token=self.store.capability(inbound.OWNER,inbound.OWNER,task['reply'],data)
        with self.assertRaises(PermissionError):self.store.use(token,inbound.OWNER,inbound.OWNER,task['reply']+1)
        self.bot.get_file.assert_not_awaited()

    async def test_partial_download_available_counts_and_native_button(self):
        task=self.fixture(3);self.store.set_task(task['id'],state='failed',progress=2)
        item=json.loads(task['payload'])['items'][-1]
        self.store.db.execute('DELETE FROM cache WHERE key=?',(digest([item['chat'],item['id'],item['media_id']]),))
        task=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        buttons=await self.controller.task_buttons(inbound.OWNER,inbound.OWNER,task['reply'],task)
        self.assertIn('📂 下載位置',[b.text for b in buttons])
        await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],self.data(task))
        self.assertIn('可用 2/3',self.bot.edit_message_text.call_args.kwargs['text'])
        self.assertIn('未有快取路徑',self.bot.edit_message_text.call_args.kwargs['text'])

    async def test_refresh_known_done_cards_once_no_unknown_or_ambiguous_overwrite(self):
        task=self.fixture(1);unknown=self.fixture(2,reply=0)
        # An active/ambiguous card sharing a reply must not be overwritten by an older DONE task.
        blocked=self.fixture(3,reply=78002)
        payload=json.loads(blocked['payload']);payload['action']='cached_photos';payload['origin_task']=blocked['id']
        other=self.store.create_task(inbound.OWNER,inbound.OWNER,78002,payload);self.store.set_task(other['id'],state='ambiguous')
        await self.controller.refresh_task_location_cards()
        replies=[c.kwargs['message_id'] for c in self.bot.edit_message_text.await_args_list]
        self.assertEqual(replies,[task['reply']]);self.bot.send_message.assert_not_awaited()
        count=self.bot.edit_message_text.await_count
        await self.controller.refresh_task_location_cards();self.assertEqual(self.bot.edit_message_text.await_count,count)

    async def repair_data(self,task):
        await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],self.data(task))
        rows=self.bot.edit_message_text.call_args.kwargs['reply_markup'].inline_keyboard
        button=next(b for row in rows for b in row if '補下載遺失' in b.text)
        return self.store.use(button.callback_data,inbound.OWNER,inbound.OWNER,task['reply'])

    async def test_duplicate_done_is_historical_then_explicit_repair_preserves_history_and_paths(self):
        from types import SimpleNamespace as NS
        task=self.fixture(2);self.store.set_task(task['id'],attempt=1)
        task=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        old=dict(task);payload=json.loads(task['payload'])
        self.assertEqual(self.store.create_task(inbound.OWNER,inbound.OWNER,task['reply'],payload)['id'],task['id'])
        row=self.store.db.execute('SELECT * FROM cache ORDER BY rowid LIMIT 1').fetchone();missing=Path(row['path']);missing.unlink()
        await self.controller.render_task(inbound.OWNER,inbound.OWNER,task['reply'],task)
        text=self.bot.edit_message_text.call_args.kwargs['text'];self.assertIn('歷史已完成 2/2',text);self.assertIn('目前可用 1/2；遺失 1',text)
        data=await self.repair_data(task)
        await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        repair=self.store.claim();self.assertNotEqual(repair['id'],task['id'])
        current=json.loads(repair['payload']);self.assertEqual(len(current['items']),1)
        self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER),old)
        with self.assertRaises(ValueError):await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        selected=current['items'][0]
        async def download(file,progress_callback):file.write(b'data');await progress_callback(4,4);return file
        message=NS(media=object(),download_media=AsyncMock(side_effect=download))
        self.client.get_messages=AsyncMock(return_value=message);self.client.get_entity=AsyncMock(return_value=NS(title='source'))
        with patch('tg_forwarder.control.source.item',return_value={**selected,'archive_meta':{'title':'source','filename':'same.jpg'}}):await self.engine.execute(repair)
        self.assertEqual(self.store.task(repair['id'],inbound.OWNER,inbound.OWNER)['state'],'done')
        self.assertEqual(missing.read_bytes(),b'data');self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER),old)
        self.assertEqual(json.loads(self.store.get_meta('task-last-run:'+repair['id'])),{'attempt':1,'fresh':1,'reused':0})
        self.client.get_messages.reset_mock()
        restored=await self.engine.repair_destination(repair,current,selected)
        self.assertEqual(restored,missing)
        await self.engine.download(repair,current,selected,0)
        self.client.get_messages.assert_not_awaited()
        missing.write_bytes(b'evil')
        with self.assertRaises(ValueError):await self.engine.repair_destination(repair,current,selected)
        self.bot.get_file.assert_not_awaited()
        # Another deletion is a NEW explicit repair round, not dedup to the old DONE repair.
        missing.unlink();next_data=await self.repair_data(task)
        await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],next_data)
        self.assertNotEqual(self.store.claim()['id'],repair['id'])

    async def test_repair_rejects_stale_inventory_and_unsafe_pin_before_source_rpc(self):
        task=self.fixture(2);rows=self.store.db.execute('SELECT * FROM cache ORDER BY rowid').fetchall()
        missing=Path(rows[0]['path']);missing.unlink();data=await self.repair_data(task)
        # A changed missing set requires another explicit confirmation.
        Path(rows[1]['path']).unlink()
        with self.assertRaises(ValueError):await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],1)
        data=await self.repair_data(task);await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        repair=self.store.claim();payload=json.loads(repair['payload'])
        entry=payload['missing_repair']['entries'][0];entry['relative']='../../outside'
        with self.assertRaises((ValueError,PermissionError)):await self.engine.repair_destination(repair,payload,payload['items'][0])
        self.bot.get_file.assert_not_awaited()

    async def test_queued_missing_repair_rejects_same_size_occupied_path_before_source(self):
        task=self.fixture(1);row=self.store.db.execute('SELECT * FROM cache').fetchone()
        path=Path(row['path']);path.unlink();data=await self.repair_data(task)
        await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        path.write_bytes(b'evil');path.chmod(0o600)
        repair=self.store.claim()
        self.client.get_messages=AsyncMock(side_effect=AssertionError('No source RPC'))
        await self.engine.execute(repair)
        result=self.store.task(repair['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(result['state'],'failed');self.assertIn('occupied',result['error'])
        self.assertEqual(path.read_bytes(),b'evil');self.client.get_messages.assert_not_awaited();self.bot.get_file.assert_not_awaited()
        stats=json.loads(self.store.get_meta('task-last-run:'+repair['id']))
        self.assertEqual((stats['fresh'],stats['reused']),(0,0))

    async def test_refresh_duplicate_capability_anchors_per_card_retry_and_bindings(self):
        task=self.fixture(1);self.store.meta('bot_principal',str(inbound.BOT))
        for reply in (78003,78004,78005):
            self.store.capability(inbound.OWNER,inbound.OWNER,reply,{'kind':'task','task':task['id'],'do':'details'})
        self.store.capability(inbound.OWNER,inbound.OWNER,78006,{**self.data(task),'revision':'bad'})
        self.store.capability(inbound.OWNER,inbound.OWNER,78007,{**self.data(task),'bot':inbound.BOT+1})
        self.store.capability(192114587,192114587,78008,self.data(task))
        original=self.controller.render_task
        async def render(owner,chat,reply,current):
            if reply==78004:raise RuntimeError('transient edit failure')
            await original(owner,chat,reply,current)
        with patch.object(self.controller,'render_task',side_effect=render):await self.controller.refresh_task_location_cards()
        replies=[c.kwargs['message_id'] for c in self.bot.edit_message_text.await_args_list]
        self.assertEqual(set(replies),{task['reply'],78003,78005})
        await self.controller.refresh_task_location_cards()
        replies=[c.kwargs['message_id'] for c in self.bot.edit_message_text.await_args_list]
        self.assertEqual(replies[-1],78004);self.assertEqual(len(replies),4)
        await self.controller.refresh_task_location_cards();self.assertEqual(self.bot.edit_message_text.await_count,4)
        self.bot.send_message.assert_not_awaited();self.bot.get_file.assert_not_awaited()

    async def test_held_download_lock_repair_rejects_file_created_while_waiting(self):
        task=self.fixture(1);path=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0]);path.unlink()
        data=await self.repair_data(task)
        await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        repair=self.store.claim()
        self.client.get_messages=AsyncMock(side_effect=AssertionError('No source RPC'))
        await self.engine.download_lock.acquire()
        job=asyncio.create_task(self.engine.execute(repair))
        try:
            for _ in range(1000):
                if self.engine.download_lock._waiters:break
                await asyncio.sleep(0.001)
            self.assertTrue(self.engine.download_lock._waiters,'Repair must actually be waiting for the held lock')
            self.assertFalse(job.done())
            path.write_bytes(b'evil');path.chmod(0o600)
        finally:self.engine.download_lock.release()
        await job
        result=self.store.task(repair['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(result['state'],'failed');self.assertIn('occupied',result['error'])
        self.assertEqual(path.read_bytes(),b'evil');self.client.get_messages.assert_not_awaited();self.bot.get_file.assert_not_awaited()
        stats=json.loads(self.store.get_meta('task-last-run:'+repair['id']))
        self.assertEqual((stats['fresh'],stats['reused']),(0,0))

    async def test_repair_reuse_rechecks_receipt_after_publish_await(self):
        from tg_forwarder.canonical_media import fingerprint
        task=self.fixture(1);row=self.store.db.execute('SELECT * FROM cache').fetchone();path=Path(row['path']);path.unlink()
        data=await self.repair_data(task);await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        repair=self.store.claim();payload=json.loads(repair['payload']);item=payload['items'][0]
        path.write_bytes(b'data');path.chmod(0o600);info=path.stat();meta=payload['missing_repair']
        binding={'task':repair['id'],'origin':meta['origin'],'revision':meta['revision'],'key':row['key'],'path':str(path),'size':4}
        self.store.meta('repair-publication:'+repair['id']+':'+row['key'],encode({'binding':binding,'proof':fingerprint(path),'identity':[info.st_dev,info.st_ino]}))
        async def publish(selected,destination):
            await asyncio.sleep(0)
            destination.write_bytes(b'evil')
        with patch.object(self.engine,'publish',side_effect=publish):
            with self.assertRaises(ValueError):await self.engine.download(repair,payload,item,0)
        self.bot.get_file.assert_not_awaited()

    async def test_fresh_repair_rechecks_after_publish_before_done(self):
        from types import SimpleNamespace as NS
        task=self.fixture(1);path=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0]);path.unlink()
        data=await self.repair_data(task);await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        repair=self.store.claim();payload=json.loads(repair['payload']);selected=payload['items'][0]
        async def download(file,progress_callback):file.write(b'data');await progress_callback(4,4);return file
        self.client.get_messages=AsyncMock(return_value=NS(media=object(),download_media=AsyncMock(side_effect=download)))
        self.client.get_entity=AsyncMock(return_value=NS(title='source'))
        async def publish(item,destination):
            await asyncio.sleep(0)
            destination.write_bytes(b'evil')
        with patch('tg_forwarder.control.source.item',return_value={**selected,'archive_meta':{'title':'source','filename':'same.jpg'}}),patch.object(self.engine,'publish',side_effect=publish):
            await self.engine.execute(repair)
        result=self.store.task(repair['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(result['state'],'failed');self.assertEqual(result['progress'],0)
        self.assertEqual(path.read_bytes(),b'evil');self.bot.get_file.assert_not_awaited()

    async def test_repair_final_done_rechecks_after_progress_notice(self):
        from types import SimpleNamespace as NS
        task=self.fixture(1);path=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0]);path.unlink()
        data=await self.repair_data(task);await self.controller.task_callback(inbound.OWNER,inbound.OWNER,task['reply'],data)
        repair=self.store.claim();payload=json.loads(repair['payload']);selected=payload['items'][0]
        async def download(file,progress_callback):file.write(b'data');await progress_callback(4,4);return file
        self.client.get_messages=AsyncMock(return_value=NS(media=object(),download_media=AsyncMock(side_effect=download)))
        self.client.get_entity=AsyncMock(return_value=NS(title='source'))
        async def notice(current,final=False):
            await asyncio.sleep(0)
            if not final and self.store.task(current['id'],inbound.OWNER,inbound.OWNER)['progress']:
                path.write_bytes(b'evil')
        with patch('tg_forwarder.control.source.item',return_value={**selected,'archive_meta':{'title':'source','filename':'same.jpg'}}),patch.object(self.engine,'notice',side_effect=notice):
            await self.engine.execute(repair)
        result=self.store.task(repair['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(result['state'],'failed');self.assertEqual(result['progress'],1)
        self.assertEqual(path.read_bytes(),b'evil');self.bot.get_file.assert_not_awaited()
