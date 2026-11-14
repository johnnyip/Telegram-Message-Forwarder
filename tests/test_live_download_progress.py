"""Offline canonical-layout and measured, coalesced download progress contracts."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch
from telegram import Update
from telegram.error import RetryAfter
import test_named_albums as named
import test_cached_resend as cached
import test_inbound_photos as inbound
from test_media_workbench import CHANNEL,GROUP,FakeClient
from tg_forwarder.control.input import SourceRef
from tg_forwarder.control.inbound import payload_for
from tg_forwarder.control.source import Resolver
from tg_forwarder.control.engine import Cancelled
from tg_forwarder.canonical_media import filename


class LiveProgressTests(unittest.IsolatedAsyncioTestCase):
    forbid_source=cached.CachedResendTests.forbid_source
    async def asyncSetUp(self):
        await named.NamedAlbumTests.asyncSetUp(self)
        self.client=FakeClient();self.engine.client=self.client
        self.resolver=Resolver(self.client,gate=self.gate,named_folders=True)
        self.controller.resolver=self.resolver
    asyncTearDown=named.NamedAlbumTests.asyncTearDown
    transport=named.NamedAlbumTests.transport
    original=cached.CachedResendTests.original

    async def payload(self):
        snap=await self.resolver.resolve(SourceRef('tgfanchabiao',4833))
        return {'action':'download','target':None,'snapshot':snap,'items':[snap['main'][0]]}

    async def running(self):
        payload=await self.payload();row=self.store.create_task(inbound.OWNER,inbound.OWNER,100,payload)
        return self.store.claim(),payload

    async def test_future_url_main_discussion_and_received_share_named_contract(self):
        self.client.channel.title='來源頻道';self.client.group.title='留言群'
        row=await self.original(1,video=True);payload=json.loads(row['payload'])
        paths=[Path(r[0]) for r in self.store.db.execute('SELECT path FROM cache')]
        self.assertEqual({p.parent.name for p in paths},{'來源頻道','留言群'})
        self.assertTrue(all(p.parent.parent==self.config.root for p in paths))
        self.assertEqual({p.name for p in paths},{filename(i) for i in payload['items']})
        raw=inbound.raw_update(origin='channel');raw['message']['forward_origin']['chat'].update(id=CHANNEL,title='來源頻道')
        received=payload_for(Update.de_json(raw,None),self.config.allowed,inbound.BOT)
        dest=await self.engine.primary_destination(received['snapshot'],received['items'][0])
        self.assertEqual(dest.parent,self.config.root/'來源頻道')
        self.assertEqual({i['chat'] for i in payload['items']},{CHANNEL,GROUP})
        self.assertFalse((self.config.root/'workbench').exists())

    async def test_measured_speed_eta_unknown_size_zero_elapsed_and_stale_samples(self):
        task,payload=await self.running();item=payload['items'][0]
        clock=NS(monotonic=lambda:0)
        self.engine.notify=None
        with patch('tg_forwarder.control.engine.time',clock):
            self.engine.record_progress(task,item,0,0)
            text=self.engine.progress_text(task);self.assertIn('大小未知',text);self.assertNotIn('ETA',text)
            self.assertIn('測速中',text)
            clock.monotonic=lambda:2
            self.engine.record_progress(task,item,1024,4096)
            text=self.engine.progress_text(task);self.assertIn('512 B/s',text);self.assertIn('25%',text);self.assertIn('此檔 ETA 6s',text)
            clock.monotonic=lambda:13
            self.assertIn('等待新進度',self.engine.progress_text(task));self.assertNotIn('ETA',self.engine.progress_text(task))
            self.engine.record_progress(task,item,1,None)  # Retry/regressed counter resets speed.
            self.assertNotIn('/s',self.engine.progress_text(task))
            self.assertEqual(self.engine.progress_text({**task,'attempt':task['attempt']+1}),'')
            self.assertEqual(self.engine.progress_text({**task,'state':'done'}),'')
        pending=self.engine.progress_notices.pop(task['id'],None)
        if pending:await pending

    async def test_slow_telegram_edit_does_not_block_transfer_and_final_is_last(self):
        task,payload=await self.running();started=asyncio.Event();release=asyncio.Event();downloaded=asyncio.Event();states=[]
        async def notify(current):
            states.append(current['state'])
            if not started.is_set():started.set();await release.wait()
        self.engine.notify=notify
        async def download(file,progress_callback):
            await progress_callback(0,4)
            await started.wait()
            file.write(b'data');await progress_callback(4,4);downloaded.set();return file
        self.client.main[0].download_media.side_effect=download
        work=asyncio.create_task(self.engine.execute(task))
        try:
            await asyncio.wait_for(downloaded.wait(),2)
            self.assertFalse(work.done());self.assertEqual(len(self.engine.progress_notices),1)
        finally:release.set();await asyncio.wait_for(work,3)
        self.assertEqual(states[-1],'done');self.assertEqual(self.engine.progress_notices,{})
        self.assertEqual(self.engine.transfers,{})
        self.assertEqual(self.store.task(task['id'],task['owner'],task['chat'])['state'],'done')

    async def test_coalesced_notices_three_seconds_and_429_cooldown(self):
        task,payload=await self.running();item=payload['items'][0]
        clock=NS(monotonic=lambda:100)
        self.engine.notify=AsyncMock(side_effect=RetryAfter(7))
        with patch('tg_forwarder.control.engine.time',clock):
            for done in range(4):self.engine.record_progress(task,item,done,4)
            await self.engine.progress_notices[task['id']]
            self.engine.notify.assert_awaited_once();self.assertEqual(self.engine.notice_until,107)
            clock.monotonic=lambda:106
            self.engine.record_progress(task,item,4,4);await self.engine.notice({**task,'reply':0},final=True)
            self.engine.notify.assert_awaited_once()
            clock.monotonic=lambda:108;self.engine.notify.side_effect=None
            await self.engine.notice(task,final=True);self.assertEqual(self.engine.notify.await_count,2)
        self.engine.progress_notices.clear()

    async def test_cancellation_callback_stops_before_new_progress(self):
        task,payload=await self.running()
        self.store.set_task(task['id'],state='cancelled')
        with self.assertRaises(Cancelled):self.engine.check(task)
        self.assertEqual(self.engine.transfers,{})
        await self.engine.execute(task)
        self.assertEqual(self.engine.progress_notices,{})
        self.client.main[0].download_media.assert_not_awaited()

    async def test_compact_card_detail_and_live_sample_keep_native_buttons(self):
        task,payload=await self.running();self.engine.notify=None
        self.engine.record_progress(task,payload['items'][0],1,None)
        await self.controller.render_task(task['owner'],task['chat'],100,task)
        compact=self.bot.edit_message_text.call_args.kwargs['text']
        self.assertIn('大小未知',compact);self.assertLess(len(compact),500)
        self.assertNotIn(task['id'],compact);self.assertNotIn('嘗試',compact)
        self.assertTrue(any('詳情' in b.text for row in self.bot.edit_message_text.call_args.kwargs['reply_markup'].inline_keyboard for b in row))
        await self.controller.task_callback(task['owner'],task['chat'],100,{'kind':'task','task':task['id'],'do':'details'})
        detail=self.bot.edit_message_text.call_args.kwargs['text']
        self.assertIn('嘗試 1',detail);self.assertIn(task['id'],detail)
        with self.assertRaises(PermissionError):await self.controller.task_callback(192114587,192114587,100,{'kind':'task','task':task['id'],'do':'details'})
        pending=self.engine.progress_notices.pop(task['id'],None)
        if pending:await pending

    async def test_final_429_retries_only_known_edit_once_without_source(self):
        task,payload=await self.running();self.engine.notify=AsyncMock(side_effect=[RetryAfter(1),None])
        with patch('tg_forwarder.control.engine.asyncio.sleep',new_callable=AsyncMock) as sleep:
            await self.engine.notice(task,final=True)
        self.assertEqual(self.engine.notify.await_count,2);sleep.assert_awaited_once()
        self.bot.get_file.assert_not_awaited();self.client.main[0].download_media.assert_not_awaited()
        self.engine.notice_until=0;self.engine.notify=AsyncMock(side_effect=RetryAfter(1))
        await self.engine.notice({**task,'reply':0},final=True)
        self.engine.notify.assert_awaited_once()  # Never retry a first-send outcome.

    async def test_new_attempt_clears_old_transfer_statistics(self):
        task,payload=await self.running();self.engine.notify=None
        self.engine.record_progress(task,payload['items'][0],3,4)
        pending=self.engine.progress_notices.get(task['id'])
        if pending:await pending
        self.store.set_task(task['id'],state='failed')
        self.store.retry(task['id'],task['owner'],task['chat']);retry=self.store.claim()
        self.assertEqual(retry['attempt'],task['attempt']+1)
        await self.engine.execute(retry)
        self.assertEqual(self.engine.progress_text(retry),'');self.assertEqual(self.engine.transfers,{})
        self.assertEqual(json.loads(self.store.get_meta('task-last-run:'+task['id']))['attempt'],retry['attempt'])
