import asyncio
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch
import test_flat_archive as flat
import test_inbound_photos as inbound
from tg_forwarder.control.controller import Controller
from tg_forwarder.control.source import Resolver
from tg_forwarder.control.inbound import payload_for
from telegram import Update


class NamedAlbumTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown=flat.FlatTests.asyncTearDown
    execute=flat.FlatTests.execute
    forbid_source=flat.FlatTests.forbid_source
    assert_no_source=flat.FlatTests.assert_no_source
    transport=flat.FlatTests.transport
    queue=flat.FlatTests.queue

    async def asyncSetUp(self):
        await flat.FlatTests.asyncSetUp(self)
        self.config=replace(self.config,archive_named=True)
        self.engine.config=self.config;self.controller.config=self.config;self.engine.archive.named=True
        self.engine.notify=self.controller.notify_task
        self.transport()
        async def sent(**kw):
            self.mid+=1;return NS(message_id=self.mid)
        self.bot.send_message.side_effect=sent

    def mature(self):self.store.db.execute('UPDATE inbound_notices SET due=0')

    async def add(self,mid,album='album-A',origin='hidden_user'):
        raw=inbound.raw_update(mid,origin=origin,group=album)
        raw['message']['chat'].update(first_name='Johnny',last_name='叔叔')
        raw['message']['from'].update(first_name='Johnny',last_name='叔叔')
        await self.queue(raw)

    async def drain(self,n):
        for _ in range(n):
            pending=self.store.db.execute("SELECT payload FROM tasks WHERE state IN ('queued','delayed') ORDER BY rowid LIMIT 1").fetchone()
            self.current_unique=json.loads(pending['payload'])['items'][0]['file_unique_id']
            task=await self.execute();self.assertEqual(task['state'],'done',task['error'])
            self.mature();await self.controller.inbound_notice_tick()

    async def test_actual_named_collision_keeps_folder_and_unknown_file_unchanged(self):
        raw=inbound.raw_update(89700,origin='hidden_user',group=None)
        raw['message']['chat'].update(first_name='Johnny',last_name='叔叔')
        raw['message']['from'].update(first_name='Johnny',last_name='叔叔')
        p=payload_for(Update.de_json(raw,None),self.config.allowed,inbound.BOT)
        original=self.engine.archive.file(p['snapshot'],p['items'][0])
        original.parent.mkdir(mode=0o700);original.write_bytes(b'evil')
        await self.queue(raw);task=await self.execute()
        self.assertEqual(task['state'],'done',task['error'])
        saved=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0])
        self.assertNotEqual(saved,original);self.assertEqual(saved.parent,original.parent)
        self.assertEqual(saved.read_bytes(),b'data');self.assertEqual(original.read_bytes(),b'evil')
        self.assertFalse([p for p in self.config.root.iterdir() if p.is_file()])

    async def test_ten_complete_while_initial_send_waits_generation_preserves_final_status(self):
        for mid in range(89800,89810):await self.add(mid)
        self.mature();started=asyncio.Event();release=asyncio.Event()
        async def held(**kw):
            started.set();await release.wait();return NS(message_id=12345)
        self.bot.send_message.side_effect=held
        first=asyncio.create_task(self.controller.inbound_notice_tick())
        try:
            await asyncio.wait_for(started.wait(),1)
            before=self.store.db.execute('SELECT generation FROM inbound_notices').fetchone()[0]
            for _ in range(10):
                pending=self.store.db.execute("SELECT payload FROM tasks WHERE state='queued' ORDER BY rowid LIMIT 1").fetchone()
                self.current_unique=json.loads(pending['payload'])['items'][0]['file_unique_id']
                task=await self.execute();self.assertEqual(task['state'],'done',task['error'])
            row=self.store.db.execute('SELECT state,generation FROM inbound_notices').fetchone()
            self.assertEqual(row['state'],'sending');self.assertGreater(row['generation'],before)
            release.set();await asyncio.wait_for(first,3)
        finally:
            release.set()
            if not first.done():
                first.cancel()
                await asyncio.gather(first,return_exceptions=True)
        self.assertEqual(self.store.db.execute('SELECT state FROM inbound_notices').fetchone()[0],'pending')
        await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,1)
        self.assertIn('10/10',self.bot.edit_message_text.call_args.kwargs['text'])
        self.assertEqual(self.bot.edit_message_text.call_args.kwargs['message_id'],12345)
        self.assertEqual(self.store.db.execute('SELECT state FROM inbound_notices').fetchone()[0],'sent')

    async def test_album_ten_one_send_and_edits_total_complete(self):
        for mid in range(81000,81010):await self.add(mid)
        await self.controller.inbound_notice_tick();self.bot.send_message.assert_not_awaited()
        self.mature();await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,1)
        await self.drain(10)
        self.assertEqual(self.bot.send_message.await_count,1)
        self.assertIn('10/10',self.bot.edit_message_text.call_args.kwargs['text'])
        ids={call.kwargs['message_id'] for call in self.bot.edit_message_text.await_args_list}
        self.assertEqual(len(ids),1)
        paths=[Path(r[0]) for r in self.store.db.execute('SELECT path FROM cache')]
        self.assertTrue(all(p.parent==self.config.root/'Johnny叔叔' or p.parent==self.config.root/'Johnny 叔叔' for p in paths))
        self.assertFalse(any('2026_11' in str(p) for p in paths));self.assert_no_source()

    async def test_two_albums_two_messages_never_time_merged(self):
        for group,start in (('album-A',82000),('album-B',83000)):
            for mid in range(start,start+10):await self.add(mid,group)
        self.mature();await self.controller.inbound_notice_tick();await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,2)
        await self.drain(20);self.assertEqual(self.bot.send_message.await_count,2)
        self.assertEqual(len({r[0] for r in self.store.db.execute('SELECT reply FROM inbound_notices')}),2)

    async def test_late_split_and_controller_restart_edit_same_anchor(self):
        for mid in range(84000,84003):await self.add(mid)
        self.mature();await self.controller.inbound_notice_tick();await self.drain(3)
        anchor=self.store.db.execute('SELECT reply FROM inbound_notices').fetchone()[0]
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        self.engine.notify=self.controller.notify_task
        for mid in range(84003,84010):await self.add(mid)
        self.mature();await self.controller.inbound_notice_tick();await self.drain(7)
        self.assertEqual(self.bot.send_message.await_count,1)
        self.assertEqual(self.store.db.execute('SELECT reply FROM inbound_notices').fetchone()[0],anchor)
        self.assertIn('10/10',self.bot.edit_message_text.call_args.kwargs['text'])

    async def test_single_and_second_single_each_one_notice(self):
        await self.add(85000,None);await self.add(85001,None)
        self.mature();await self.controller.inbound_notice_tick();await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,2)

    async def test_unknown_first_send_outcome_never_resends_on_restart(self):
        await self.add(86000);self.mature()
        self.bot.send_message.side_effect=ConnectionError('ambiguous')
        await self.controller.inbound_notice_tick()
        self.assertEqual(self.store.db.execute('SELECT state FROM inbound_notices').fetchone()[0],'ambiguous')
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        await self.controller.inbound_notice_tick();self.assertEqual(self.bot.send_message.await_count,1)

    async def test_cancel_during_send_recovery_holds_uncertainty_without_new_send(self):
        await self.add(87000);self.mature();started=asyncio.Event()
        async def blocked(**kw):started.set();await asyncio.Event().wait()
        self.bot.send_message.side_effect=blocked
        task=asyncio.create_task(self.controller.inbound_notice_tick())
        await asyncio.wait_for(started.wait(),1);task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(self.store.db.execute('SELECT state FROM inbound_notices').fetchone()[0],'sending')
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        await self.controller.inbound_notice_tick()
        self.assertEqual(self.store.db.execute('SELECT state FROM inbound_notices').fetchone()[0],'ambiguous')
        self.assertEqual(self.bot.send_message.await_count,1)

    async def test_concurrent_ticks_cannot_duplicate_first_send(self):
        await self.add(88000);self.mature()
        await asyncio.gather(*(self.controller.inbound_notice_tick() for _ in range(5)))
        self.assertEqual(self.bot.send_message.await_count,1)

    async def test_album_namespace_and_unauthorized_updates_do_not_merge(self):
        await self.add(89000)
        await self.queue(inbound.raw_update(89001,uid=192114587))
        self.engine.bot_id=inbound.BOT+1;await self.add(89002)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM inbound_notices').fetchone()[0],2)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)

    async def test_named_chat_topic_direct_media_and_sanitized_collision(self):
        item={'chat':-1000000000011,'id':54,'media_id':'9','kind':'photo','archive_meta':{'title':'Chat/A','topic_title':'Topic?One','topic_id':7,'photo_id':9}}
        one=await self.engine.flat_destination({},item,'one')
        self.assertEqual(one.parent.name,'Chat_A - Topic_One');self.assertEqual(one.parent.parent,self.config.root)
        other={**item,'chat':-1000000000022}
        two=await self.engine.flat_destination({},other,'two')
        self.assertNotEqual(one.parent,two.parent)
        missing={**item,'archive_meta':{**item['archive_meta'],'topic_title':None}}
        with self.assertRaises(ValueError):self.engine.archive.file({},missing)
        item['archive_meta']['title']='..'
        item['archive_meta']['topic_title']=None;item['archive_meta']['topic_id']=None
        with self.assertRaises(ValueError):self.engine.archive.file({},item)

    async def test_actual_received_channel_origin_title_not_sender_authority(self):
        raw=inbound.raw_update(origin='channel')
        payload=payload_for(Update.de_json(raw,None),self.config.allowed,inbound.BOT)
        path=self.engine.archive.file(payload['snapshot'],payload['items'][0])
        self.assertEqual(path.parent.name,'source');self.assertEqual(payload['items'][0]['chat'],inbound.OWNER)
        self.assertEqual(payload['items'][0]['archive_meta']['origin_chat_id'],-1000000000010)

    async def test_failed_download_status_edits_album_anchor_without_per_item_send(self):
        await self.add(89500);self.mature();await self.controller.inbound_notice_tick()
        self.bot.get_file.side_effect=ValueError('failure')
        task=await self.execute();self.assertNotEqual(task['state'],'done')
        self.mature();await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,1)

    async def test_recovery_after_known_reply_only_edits_no_new_send(self):
        await self.add(89600);self.mature();await self.controller.inbound_notice_tick()
        self.store.db.execute("UPDATE inbound_notices SET state='sending'")
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        await self.controller.inbound_notice_tick();await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,1)

    async def test_actual_installed_forum_api_preserves_topic_names(self):
        from telethon import functions
        client=AsyncMock(return_value=NS(topics=[NS(id=7,title='Race topic')]))
        resolver=Resolver(client,named_folders=True)
        values=[{'archive_meta':{'title':'Forum'}}]
        msg=NS(reply_to=NS(forum_topic=True,reply_to_top_id=7,reply_to_msg_id=9))
        await resolver.topics(NS(forum=True),[msg],values)
        request=client.await_args.args[0]
        self.assertIsInstance(request,functions.messages.GetForumTopicsByIDRequest)
        self.assertEqual(values[0]['archive_meta']['topic_title'],'Race topic')
