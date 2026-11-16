import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock,patch
from tg_forwarder.domain.formatting import build_header_from_info,build_chat_message_url
from tg_forwarder.runtime import bot_runtime as br

class HeaderTests(unittest.TestCase):
    def test_comment_origin_main_post_and_exact_reply_links(self):
        info={'chat_id':-1003837422582,'msg_id':26001,'chat_title':'Discussion','sender_id':9,'sender_display':'Alice','discussion_source':{'chat_id':-1004433596337,'post_id':4833,'thread':{'chat':-1003837422582,'id':24124}}}
        text=build_header_from_info(info)
        self.assertIn('[原帖 #4833](https://t.me/c/4433596337/4833)',text)
        self.assertIn('[留言 #26001](https://t.me/c/4433596337/4833?comment=26001)',text)
        self.assertIn('https://t.me/c/3837422582/26001',text)
    def test_private_bot_has_no_fake_public_message_permalink(self):
        self.assertIsNone(build_chat_message_url({'chat_id':7750751422,'chat_username':'fcmgjqrBot','msg_id':934928}))
    def test_public_source_permalink_unchanged(self):
        self.assertEqual(build_chat_message_url({'chat_id':-100123,'chat_username':'@public','msg_id':5}),'https://t.me/public/5')
    def test_missing_comment_origin_does_not_fabricate_link(self):
        self.assertNotIn('?comment=',build_header_from_info({'chat_title':'src','discussion_source':{'chat_id':-100123},'msg_id':3}))

class TopicTests(unittest.IsolatedAsyncioTestCase):
    async def test_topic_only_photo_caption_contains_topic_link_before_send(self):
        captured=[]
        async def send(*a,**kw):captured.append(a[3]);return NS(message_id=12)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'photo.jpg';path.write_bytes(b'data')
            info={'chat_id':-1003,'msg_id':2,'chat_title':'source','sender_id':9,'sender_username':'alice','sender_display':'Alice','_media_type':'photo'}
            with patch.object(br,'resolve_topic_thread_id',AsyncMock(return_value=7)),patch.object(br,'bot_send_file',send),patch.object(br,'FORUM_TOPIC_DELIVERY_MODE','topic_only'):
                result=await br.send_file_via_bot(object(),-100123,path,'caption',info,{'name':'r'},'new',bot_send_semaphore=asyncio.Semaphore(1),upload_max_bytes=999,log=lambda v:None)
        self.assertTrue(result);self.assertIn('[Topic](https://t.me/c/123/7)',captured[0]);self.assertEqual(len(captured),1)
    async def test_topic_footer_updates_without_duplicate(self):
        text=await br._append_topic_link('caption',-100123,7)
        text=await br._append_topic_link(text,-100123,7,12)
        self.assertEqual(text.count('[Topic]'),1);self.assertIn('/7/12',text)
    async def test_negative_forum_cache_refreshes(self):
        target=-100987654;bot=NS(get_chat=AsyncMock(return_value=NS(is_forum=True)))
        br._FORUM_CHAT_CACHE[target]=False;br._FORUM_CHAT_CACHE_AT[target]=0
        try:
            with patch.object(br,'get_topic_mapping',AsyncMock(return_value={'message_thread_id':7,'title':'@alice (9)'})):
                self.assertEqual(await br.resolve_topic_thread_id(bot,target,{'sender_id':9,'sender_username':'alice'},log=lambda v:None),7)
            bot.get_chat.assert_awaited_once()
        finally:br._FORUM_CHAT_CACHE.pop(target,None);br._FORUM_CHAT_CACHE_AT.pop(target,None)
    def test_other_supported_media_can_create_topic_without_plain_text_change(self):
        for kind in ['document','audio','animation','voice','video_note','sticker']:
            self.assertTrue(br._should_create_topic({'_media_type':kind}))
        self.assertFalse(br._should_create_topic({}))
