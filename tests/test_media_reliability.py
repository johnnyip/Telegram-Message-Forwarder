import asyncio
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch
from telegram.error import BadRequest, NetworkError, RetryAfter
from telethon import types
from tg_forwarder.domain.media import media_type, skip_message_kind
from tg_forwarder.delivery import bot_sender as sender
from tg_forwarder.delivery import confirmed_copy as copies
from tg_forwarder.runtime import bot_runtime as runtime
from tg_forwarder.shared_media import SharedMedia, AmbiguousDelivery
from test_shared_media import Redis

class ReliabilityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'image.jpg';self.path.write_bytes(b'photo')
        self.logs=[]
        self.redis=Redis()
        self.bridge=SharedMedia(self.redis,Path(self.tmp.name)/'archive',account_rate_key='test',min_free_bytes=0,rate_interval=.001)
        self.info={'chat_id':1,'msg_id':2,'chat_title':'source','sender_id':3,'sender_display':'sender','sender_username':'sender',
                   '_delivery_identities':[['1',2,'revision']]}

    def test_preview_is_text_only_without_downloadable_photo_or_document(self):
        msg=NS(media=types.MessageMediaWebPage(types.WebPageEmpty(id=1)),photo=None,document=None)
        self.assertEqual(media_type(msg),'text')
        self.assertEqual(skip_message_kind(msg,{'text'}),'text')
        msg.photo=NS(id=123)
        self.assertEqual(media_type(msg),'photo')

    def test_caption_bound_is_utf16_safe_preserves_tail_and_reports(self):
        original='source '+chr(0x1f600)*1000+' topic https://t.me/c/123/456'
        text,short=sender.fit_caption(original,self.logs.append)
        self.assertTrue(short)
        self.assertLessEqual(len(text.encode('utf-16-le'))//2,1024)
        self.assertTrue(text.startswith('source'))
        self.assertTrue(text.endswith('https://t.me/c/123/456'))
        self.assertEqual(self.logs[-1]['op'],'caption_bounded')
        self.assertEqual(sender.fit_caption('normal'),('normal',False))

    async def test_long_file_caption_uses_plain_and_never_exceeds_limit(self):
        bot=NS(send_photo=AsyncMock(return_value=NS(message_id=9)))
        await sender.bot_send_file(bot,4,str(self.path),'*'+chr(0x1f600)*1200+'*',media_type='photo')
        kwargs=bot.send_photo.await_args.kwargs
        self.assertIsNone(kwargs['parse_mode'])
        self.assertLessEqual(len(kwargs['caption'].encode('utf-16-le'))//2,1024)
        bot.send_photo.assert_awaited_once()

    async def test_network_file_failure_never_changes_representation_or_resends(self):
        bot=NS(send_photo=AsyncMock(side_effect=NetworkError('Gateway Timeout')),send_document=AsyncMock())
        with self.assertRaises(NetworkError):
            await sender.bot_send_file(bot,4,str(self.path),'caption',media_type='photo')
        bot.send_photo.assert_awaited_once();bot.send_document.assert_not_awaited()

    async def test_definite_parse_rejection_can_retry_plain(self):
        bot=NS(send_photo=AsyncMock(side_effect=[BadRequest("Can't parse entities"),NS(message_id=9)]))
        result=await sender.bot_send_file(bot,4,str(self.path),'caption',media_type='photo')
        self.assertEqual(result.message_id,9);self.assertEqual(bot.send_photo.await_count,2)
        self.assertIsNone(bot.send_photo.await_args.kwargs['parse_mode'])

    async def test_album_caption_bounded_before_first_request(self):
        bot=NS(send_media_group=AsyncMock(return_value=[NS(message_id=9),NS(message_id=10)]))
        await sender.bot_send_album(bot,4,[str(self.path)]*2,chr(0x1f600)*1500,media_types=['photo','photo'])
        group=bot.send_media_group.await_args.kwargs['media']
        self.assertLessEqual(len(group[0].caption.encode('utf-16-le'))//2,1024)
        self.assertIsNone(group[0].parse_mode);bot.send_media_group.assert_awaited_once()

    async def test_large_retry_after_does_not_block_send_lane(self):
        send=AsyncMock(side_effect=RetryAfter(3600))
        with patch.object(runtime.asyncio,'sleep',AsyncMock()) as sleep:
            with self.assertRaises(RetryAfter):await runtime._call_with_retry(send)
            sleep.assert_not_awaited()
        send.assert_awaited_once()

    async def test_copy_receipt_reused_and_general_thread_independent(self):
        send=AsyncMock(return_value=NS(message_id=42))
        with patch.object(copies,'configured_bridge',AsyncMock(return_value=self.bridge)),patch.dict(os.environ,{'MEDIA_DELIVERY_IDENTITY':'bot:test'}):
            first=await copies.confirmed_copy(send,info=self.info,target=4,thread=8,caption='caption')
            again=await copies.confirmed_copy(send,info=self.info,target=4,thread=8,caption='caption')
            self.assertEqual(first.message_id,again.message_id);send.assert_awaited_once()
            await copies.confirmed_copy(send,info=self.info,target=4,thread=None,caption='caption')
            self.assertEqual(send.await_count,2)

    async def test_uncertain_copy_is_fenced_not_retried(self):
        send=AsyncMock(side_effect=NetworkError('Gateway Timeout'))
        with patch.object(copies,'configured_bridge',AsyncMock(return_value=self.bridge)),patch.dict(os.environ,{'MEDIA_DELIVERY_IDENTITY':'bot:test'}):
            with self.assertRaises(NetworkError):await copies.confirmed_copy(send,info=self.info,target=4,thread=8,caption='c')
            with self.assertRaises(AmbiguousDelivery):await copies.confirmed_copy(send,info=self.info,target=4,thread=8,caption='c')
        send.assert_awaited_once()

    async def test_rejected_copy_can_retry_after_fix_without_poisoning_claim(self):
        send=AsyncMock(side_effect=[BadRequest('invalid'),NS(message_id=42)])
        with patch.object(copies,'configured_bridge',AsyncMock(return_value=self.bridge)),patch.dict(os.environ,{'MEDIA_DELIVERY_IDENTITY':'bot:test'}):
            with self.assertRaises(BadRequest):await copies.confirmed_copy(send,info=self.info,target=4,thread=None,caption='c')
            result=await copies.confirmed_copy(send,info=self.info,target=4,thread=None,caption='c')
            self.assertEqual(result.message_id,42)

    async def test_incomplete_album_response_is_not_success(self):
        send=AsyncMock(return_value=[NS(message_id=42)])
        info=dict(self.info,_delivery_identities=[['1',2,'revision'],['1',3,'revision2']])
        with patch.object(copies,'configured_bridge',AsyncMock(return_value=self.bridge)),patch.dict(os.environ,{'MEDIA_DELIVERY_IDENTITY':'bot:test'}):
            with self.assertRaises(AmbiguousDelivery):await copies.confirmed_copy(send,info=info,target=4,thread=None,caption='c',album=True)

    async def test_topic_timeout_does_not_degrade_to_general_resend(self):
        send=AsyncMock(side_effect=NetworkError('Gateway Timeout'))
        with patch.object(runtime,'resolve_topic_thread_id',AsyncMock(return_value=8)),patch.object(runtime,'_append_topic_link',AsyncMock(return_value='c')),patch.object(runtime,'_send_copy',send):
            with self.assertRaises(NetworkError):
                await runtime.send_file_via_bot(object(),4,self.path,'c',self.info,{'name':'r'},'new',bot_send_semaphore=asyncio.Semaphore(1),log=self.logs.append)
        send.assert_awaited_once()


    async def test_album_fallback_partial_success_raises_not_complete(self):
        send=AsyncMock(side_effect=[NS(message_id=42),NetworkError('Gateway Timeout')])
        with patch.object(runtime,'resolve_topic_thread_id',AsyncMock(return_value=None)),patch.object(runtime,'_append_topic_link',AsyncMock(return_value='c')),patch.object(runtime,'_send_copy',send):
            with self.assertRaises(NetworkError):
                await runtime.send_album_via_bot(object(),4,[str(self.path)]*2,'c',dict(self.info,_album_snapshots=[{'media_type':'video'}]*2),{'name':'r'},'new',bot_send_semaphore=asyncio.Semaphore(1),album_max_total_bytes=1,upload_max_bytes=1_000_000,log=self.logs.append)
        self.assertEqual(send.await_count,2)

    async def test_album_fallback_topic_timeout_never_sends_general(self):
        send=AsyncMock(side_effect=NetworkError('Gateway Timeout'))
        with patch.object(runtime,'resolve_topic_thread_id',AsyncMock(return_value=8)),patch.object(runtime,'_append_topic_link',AsyncMock(return_value='c')),patch.object(runtime,'_send_copy',send):
            with self.assertRaises(NetworkError):
                await runtime.send_album_via_bot(object(),4,[str(self.path)]*2,'c',dict(self.info,_album_snapshots=[{'media_type':'video'}]*2),{'name':'r'},'new',bot_send_semaphore=asyncio.Semaphore(1),album_max_total_bytes=1,upload_max_bytes=1_000_000,log=self.logs.append)
        send.assert_awaited_once()


    async def test_complete_fallback_produces_spool_acceptable_per_copy_receipts(self):
        from tg_forwarder.spool_lifecycle import Spool,code
        for mode,thread,expected_topic,expected_general in [('topic_only',8,[42,43],None),('mirror',8,[42,44],[43,45]),('mirror',None,None,[42,43])]:
            with self.subTest(mode=mode,thread=thread):
                send=AsyncMock(side_effect=[RuntimeError('album_too_large_for_media_group')]+[NS(message_id=i) for i in range(42,50)])
                with patch.object(runtime,'FORUM_TOPIC_DELIVERY_MODE',mode),patch.object(runtime,'resolve_topic_thread_id',AsyncMock(return_value=thread)),patch.object(runtime,'_append_topic_link',AsyncMock(return_value='c')),patch.object(runtime,'_send_copy',send):
                    result=await runtime.send_album_via_bot(object(),4,[str(self.path)]*2,'c',dict(self.info,_album_snapshots=[{'media_type':'photo'}]*2),{'name':'r'},'new',bot_send_semaphore=asyncio.Semaphore(1),album_max_total_bytes=1,upload_max_bytes=1000000,log=self.logs.append)
                self.assertEqual(result.delivery_kind,'album');self.assertEqual(result.topic_message_ids,expected_topic);self.assertEqual(result.general_message_ids,expected_general)
                self.assertEqual(result.sent_message_ids,expected_general or expected_topic)
                m=Spool(self.tmp.name);job={'snapshots':[{},{}],'spool_ref':'proof'}
                with m.db() as c:c.execute('INSERT OR REPLACE INTO targets VALUES(?,?,?)',('proof',code(['r',4]),code({'state':'inflight'})))
                self.assertTrue(m.target_result(job,'r',4,result))

    async def test_fallback_topic_retry_after_never_degrades_to_general(self):
        send=AsyncMock(side_effect=[RuntimeError('album_too_large_for_media_group'),RetryAfter(300)])
        with patch.object(runtime,'resolve_topic_thread_id',AsyncMock(return_value=8)),patch.object(runtime,'_append_topic_link',AsyncMock(return_value='c')),patch.object(runtime,'_send_copy',send):
            with self.assertRaises(RetryAfter):
                await runtime.send_album_via_bot(object(),4,[str(self.path)]*2,'c',dict(self.info,_album_snapshots=[{'media_type':'photo'}]*2),{'name':'r'},'new',bot_send_semaphore=asyncio.Semaphore(1),album_max_total_bytes=1,upload_max_bytes=1000000,log=self.logs.append)
        self.assertEqual(send.await_count,2)
