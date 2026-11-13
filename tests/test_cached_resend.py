import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch
import socket
from dataclasses import replace
from telegram import Update
from telegram.error import BadRequest
from tg_forwarder.control.config import Config
from tg_forwarder.control.store import Store, digest
from tg_forwarder.control.engine import Engine, RateGate
from tg_forwarder.control.controller import Controller
from tg_forwarder.control.delivery import BotDelivery
from tg_forwarder.control.source import Resolver
from tg_forwarder.control.input import SourceRef
from test_media_workbench import FakeClient

OWNER = 5268216341
OLD_OWNER = 192114587


class CachedResendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.net=patch.object(socket.socket,"connect",side_effect=AssertionError("network forbidden"))
        self.net.start();self.addCleanup(self.net.stop)
        self.peer=patch("tg_forwarder.control.source.utils.get_peer_id",side_effect=lambda value:value.key)
        self.peer.start();self.addCleanup(self.peer.stop)
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.config=Config(frozenset({OWNER}),((OWNER,None),),self.root/'archive',self.root/'state.db',reserve_bytes=0)
        self.store=Store(self.config.db)
        self.client=FakeClient()
        self.bot=NS(send_message=AsyncMock(return_value=NS(message_id=100)),edit_message_text=AsyncMock(),answer_callback_query=AsyncMock())
        self.mid=900
        async def album(**kwargs):
            self.mid+=len(kwargs['media'])
            return [NS(message_id=self.mid+n) for n in range(len(kwargs['media']))]
        async def photo(**kwargs):
            self.mid+=1;return NS(message_id=self.mid)
        self.bot.send_media_group=AsyncMock(side_effect=album)
        self.bot.send_photo=AsyncMock(side_effect=photo)
        self.bot.send_video=AsyncMock()
        self.gate=RateGate(self.store)
        self.engine=Engine(self.config,self.store,self.client,self.gate,delivery=BotDelivery(self.bot))
        self.resolver=Resolver(self.client,gate=self.gate)
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)

    async def asyncTearDown(self):
        self.store.close();self.temp.cleanup()

    async def original(self,photos=11,video=False,action='download'):
        snap=await self.resolver.resolve(SourceRef('tgfanchabiao',4833))
        values=[i for i in snap['main']+snap['discussion'] if i['kind']=='photo'][:photos]
        if video:values += [next(i for i in snap['discussion'] if i['kind']=='video')]
        p={'snapshot':snap,'items':values,'action':action,'target':[OWNER,None] if action=='both' else None}
        row=self.store.create_task(OWNER,OWNER,100,p)
        await self.engine.execute(self.store.claim())
        row=self.store.task(row['id'],OWNER,OWNER)
        self.assertEqual(row['state'],'done',row['error'])
        await self.controller.render_task(OWNER,OWNER,100,row)
        return row

    def button(self):
        return next(b.callback_data for r in self.bot.edit_message_text.call_args.kwargs['reply_markup'].inline_keyboard for b in r if '傳送已下載圖片' in b.text)

    async def click(self,token,uid=OWNER,chat=OWNER,reply=100):
        update=Update.de_json({'update_id':44,'callback_query':{'id':'cb','chat_instance':'fixed','data':token,
            'from':{'id':uid,'is_bot':False,'first_name':'owner'},'message':{'message_id':reply,'date':1,
            'chat':{'id':chat,'type':'private'},'from':{'id':999,'is_bot':True,'first_name':'bot'},'text':'task'}}},None)
        await self.controller.handle(update)

    def forbid_source(self):
        self.client.get_entity=AsyncMock(side_effect=AssertionError('source fetch forbidden'))
        self.client.get_messages=AsyncMock(side_effect=AssertionError('source fetch forbidden'))
        for msg in self.client.main+self.client.replies:msg.download_media.reset_mock()
        self.bot.send_media_group.reset_mock();self.bot.send_photo.reset_mock();self.bot.send_video.reset_mock()

    async def execute(self):
        row=self.store.claim();self.assertIsNotNone(row)
        await self.engine.execute(row)
        return self.store.task(row['id'],OWNER,OWNER)

    def assert_no_source(self):
        self.client.get_entity.assert_not_awaited();self.client.get_messages.assert_not_awaited()
        for msg in self.client.main+self.client.replies:msg.download_media.assert_not_awaited()

    async def test_actual_owner_callback_eleven_photos_10_plus_1_without_source(self):
        original=await self.original(video=True)
        token=self.button();self.forbid_source()
        await self.click(token)
        row=await self.execute()
        self.assertEqual(row['state'],'done',row['error'])
        self.assertEqual(len(json.loads(row['payload'])['items']),11)
        self.assertEqual(len(self.bot.send_media_group.call_args.kwargs['media']),10)
        self.bot.send_photo.assert_awaited_once();self.bot.send_video.assert_not_awaited()
        self.assertEqual(self.bot.send_photo.call_args.kwargs['chat_id'],OWNER)
        self.assertIsNone(self.bot.send_photo.call_args.kwargs['message_thread_id'])
        self.assert_no_source()
        self.assertTrue(all(Path(r[0]).is_file() for r in self.store.db.execute('SELECT path FROM cache')))

    async def test_old_owner_cross_chat_reply_expiry_tampering_no_task_created(self):
        await self.original(1);token=self.button();self.forbid_source()
        for uid,chat,reply in ((OLD_OWNER,OWNER,100),(OWNER,123,100),(OWNER,OWNER,101)):
            await self.click(token,uid,chat,reply)
        await self.click('forged-token')
        self.store.db.execute('UPDATE callbacks SET expires=0 WHERE id=?',(token,))
        await self.click(token)
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),1)
        self.assert_no_source()

    async def test_double_click_and_restart_do_not_resend(self):
        await self.original(1);token=self.button();self.forbid_source()
        await self.click(token);await self.click(token)
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),2)
        row=await self.execute();self.assertEqual(row['state'],'done')
        self.store.recover()
        await self.click(token);self.assertIsNone(self.store.claim())
        self.store.set_task(row['id'],state='queued',due=0)
        self.assertEqual((await self.execute())['state'],'done')
        self.bot.send_photo.assert_awaited_once();self.assert_no_source()

    async def test_missing_archive_fails_before_any_send_never_downloads(self):
        original=await self.original();token=self.button()
        p=json.loads(original['payload']);path,_=self.engine.cached_file(p['items'][-1]);path.unlink()
        self.forbid_source();await self.click(token)
        row=await self.execute()
        self.assertEqual(row['state'],'failed');self.assertIn('不會重新下載',row['error'])
        self.bot.send_media_group.assert_not_awaited();self.bot.send_photo.assert_not_awaited();self.assert_no_source()
        self.assertTrue(self.engine.cached_file(p['items'][0])[0].is_file())

    async def test_invalid_archive_size_and_path_fail_closed(self):
        original=await self.original(1);token=self.button();self.forbid_source()
        self.store.db.execute('UPDATE cache SET path=?',(str(self.root/'outside.jpg'),))
        await self.click(token);row=await self.execute()
        self.assertEqual(row['state'],'failed');self.bot.send_photo.assert_not_awaited();self.assert_no_source()

    async def test_normal_both_receipts_do_not_suppress_explicit_resend(self):
        await self.original(1,action='both');token=self.button();self.forbid_source()
        await self.click(token);self.assertEqual((await self.execute())['state'],'done')
        self.bot.send_photo.assert_awaited_once();self.assert_no_source()
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM operations WHERE state='done'").fetchone()[0],2)

    async def test_definite_rejection_retries_existing_task_safely(self):
        await self.original(1);token=self.button();self.forbid_source()
        self.bot.send_photo.side_effect=BadRequest('denied')
        await self.click(token);row=await self.execute();self.assertEqual(row['state'],'failed')
        await self.controller.render_task(OWNER,OWNER,100,row)
        retry=next(b.callback_data for r in self.bot.edit_message_text.call_args.kwargs['reply_markup'].inline_keyboard for b in r if '重試失敗' in b.text)
        self.bot.send_photo.side_effect=None;self.bot.send_photo.return_value=NS(message_id=902)
        await self.click(retry);self.assertEqual((await self.execute())['state'],'done');self.assert_no_source()

    async def test_uncertain_send_stays_ambiguous_on_button_replay_and_restart(self):
        await self.original(1);token=self.button();self.forbid_source()
        self.bot.send_photo.side_effect=asyncio.CancelledError()
        await self.click(token);row=await self.execute();self.assertEqual(row['state'],'ambiguous')
        self.store.recover();await self.click(token);self.assertIsNone(self.store.claim())
        self.store.set_task(row['id'],state='queued',due=0)
        self.assertEqual((await self.execute())['state'],'ambiguous')
        self.bot.send_photo.assert_awaited_once();self.assert_no_source()

    async def test_target_allowlist_removal_and_group_context_are_not_authorized(self):
        row=await self.original(1);token=self.button();self.forbid_source()
        self.engine.config=replace(self.config,targets=())
        await self.click(token)
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),1)
        self.engine.config=self.config
        self.store.db.execute('UPDATE tasks SET chat=-100123 WHERE id=?',(row['id'],))
        group=self.store.task(row['id'],OWNER,-100123)
        with self.assertRaises(PermissionError):self.engine.cached_photo_payload(group)
        self.assert_no_source()

    async def test_video_only_has_no_button(self):
        snap=await self.resolver.resolve(SourceRef('tgfanchabiao',4833))
        p={'action':'download','snapshot':snap,'items':[next(i for i in snap['discussion'] if i['kind']=='video')],'target':None}
        row=self.store.create_task(OWNER,OWNER,100,p);await self.engine.execute(self.store.claim())
        await self.controller.render_task(OWNER,OWNER,100,row)
        with self.assertRaises(StopIteration):self.button()

    async def test_old_persisted_task_refresh_via_tasks_then_details_needs_no_source(self):
        original=await self.original(1)
        self.forbid_source()
        self.store.close();self.store=Store(self.config.db);self.store.recover()
        self.engine=Engine(self.config,self.store,self.client,RateGate(self.store),delivery=BotDelivery(self.bot))
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        update=Update.de_json({'update_id':50,'message':{'message_id':10,'date':1,'chat':{'id':OWNER,'type':'private'},
            'from':{'id':OWNER,'is_bot':False,'first_name':'owner'},'text':'/tasks'}},None)
        await self.controller.handle(update)
        details=next(b.callback_data for r in self.bot.edit_message_text.call_args.kwargs['reply_markup'].inline_keyboard for b in r if b.text.startswith('詳情 '))
        await self.click(details)
        token=self.button()
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),1)
        self.bot.send_photo.assert_not_awaited()
        await self.click(token)
        self.assertEqual((await self.execute())['state'],'done');self.assert_no_source()

    async def test_shared_receipt_is_distinct_from_normal_and_replays_without_send(self):
        from tg_forwarder.shared_media import SharedMedia
        from test_shared_media import Redis
        for msg in self.client.main:self.assertTrue(msg.photo);msg.photo=NS(id=80000+msg.id)
        bridge=SharedMedia(Redis(),self.root/'shared',account_rate_key='offline-fixture',min_free_bytes=0,rate_interval=.001)
        self.gate=RateGate(self.store,bridge)
        self.engine=Engine(self.config,self.store,self.client,self.gate,delivery=BotDelivery(self.bot),bridge=bridge,bot_id=6317800517)
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        await self.original(1,action='both');token=self.button();self.forbid_source()
        await self.click(token);row=await self.execute();self.assertEqual(row['state'],'done',row['error'])
        self.bot.send_photo.assert_awaited_once()
        self.assertEqual(len([key for key in bridge.redis.data if ':done:' in key]),2)
        payload=json.loads(row['payload']);key=self.engine.operation_key(payload,payload['items'][0])
        self.store.db.execute('DELETE FROM operations WHERE key=?',(key,))
        self.store.set_task(row['id'],state='queued',due=0)
        self.assertEqual((await self.execute())['state'],'done')
        self.bot.send_photo.assert_awaited_once();self.assert_no_source()

    async def test_corrupt_and_unknown_cached_entry_fail_without_fallback(self):
        original=await self.original(1);token=self.button();payload=json.loads(original['payload'])
        path,_=self.engine.cached_file(payload['items'][0]);path.write_bytes(b'bad')
        self.forbid_source();await self.click(token)
        row=await self.execute();self.assertEqual(row['state'],'failed')
        self.assertEqual(path.read_bytes(),b'bad');self.assert_no_source()
        self.store.db.execute('DELETE FROM cache')
        self.store.set_task(row['id'],state='queued',due=0)
        self.assertEqual((await self.execute())['state'],'failed')
        self.bot.send_photo.assert_not_awaited();self.assert_no_source()

    async def test_normal_forward_identity_is_unchanged(self):
        original=await self.original(1,action='both')
        p=json.loads(original['payload']);selected=p['items'][0]
        self.assertEqual(self.engine.operation_key(p,selected),digest(['forward',p['target'],selected['chat'],selected['id'],selected.get('media_id','')]))
