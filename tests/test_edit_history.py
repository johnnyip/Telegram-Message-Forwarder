import asyncio
import copy
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from tg_forwarder.delivery import edit_updates as edits


class EditHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        edits._history_lock = asyncio.Lock()
        self.info = {'chat_id': 1, 'msg_id': 2, 'grouped_id': 3, 'edit_date': '2026-11-10 15:39:00+00:00'}
        self.mapping = {'targets': {'-1007': {'message_id': 11, 'general_message_id': 11, 'topic_message_id': 12, 'message_thread_id': 6, 'source_text': 'old', 'delivery_kind': 'file'}}}
        self.messages = {11: 'OLD GENERAL', 12: 'OLD TOPIC'}
        self.calls = []
        self.replies = []
        self.logs = []
        async def fetch(chat, mid):
            value = self.messages.get(mid)
            return SimpleNamespace(message=value, entities=[]) if value is not None else None
        async def edit(**kw):
            self.calls.append(kw)
            # HTML entities are empty in these fixtures; the separate formatting
            # test checks that old entities are actually carried into the request.
            import html
            self.messages[kw['message_id']] = html.unescape(kw.get('text', kw.get('caption')))
        async def send(**kw):
            self.replies.append(kw)
            return SimpleNamespace(message_id=100+len(self.replies))
        async def store(chat, mid, target, payload):
            self.mapping['targets'][str(target)] = copy.deepcopy(payload)
        self.fetch = fetch
        self.bot = SimpleNamespace(edit_message_text=AsyncMock(side_effect=edit), edit_message_caption=AsyncMock(side_effect=edit), send_message=AsyncMock(side_effect=send))
        self.patches = [patch.object(edits, 'get_message_mapping', AsyncMock(side_effect=lambda *a: copy.deepcopy(self.mapping))), patch.object(edits, 'get_album_mapping', AsyncMock(side_effect=lambda *a: copy.deepcopy(self.mapping))), patch.object(edits, 'store_message_mapping', AsyncMock(side_effect=store)), patch.object(edits, 'store_album_mapping', AsyncMock(side_effect=store))]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)

    async def apply(self, text='new', kind='caption', info=None):
        fn = {'caption': edits.edit_forwarded_media_caption, 'text': edits.edit_forwarded_text, 'album': edits.edit_forwarded_album_caption}[kind]
        return await fn(self.bot, info or self.info, text, self.logs.append, original_fetcher=self.fetch)

    async def test_caption_keeps_both_copies_and_original(self):
        self.assertTrue(await self.apply())
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(self.messages[11].startswith('OLD GENERAL'))
        self.assertTrue(self.messages[12].startswith('OLD TOPIC'))
        self.assertIn('[Edit 2026-11-10 23:39:00', self.messages[11])
        self.assertTrue(self.messages[11].endswith('new'))

    async def test_multiple_edits_keep_all_versions(self):
        await self.apply('v2')
        await self.apply('v3', info={**self.info, 'edit_date': '2026-11-10 15:40:00+00:00'})
        self.assertIn('OLD GENERAL', self.messages[11])
        self.assertIn('v2', self.messages[11])
        self.assertTrue(self.messages[11].endswith('v3'))

    async def test_duplicate_and_media_only_edit_are_noop(self):
        await self.apply('old')
        self.assertEqual(self.calls, [])
        await self.apply('new')
        await self.apply('new')
        self.assertEqual(len(self.calls), 2)

    async def test_text_uses_text_api(self):
        await self.apply(kind='text')
        self.assertEqual(self.bot.edit_message_text.await_count, 2)
        self.bot.edit_message_caption.assert_not_awaited()

    async def test_album_only_edits_first_caption_per_copy(self):
        self.mapping['targets']['-1007']['message_ids'] = [11, 13, 14]
        await self.apply(kind='album')
        self.assertEqual({c['message_id'] for c in self.calls}, {11, 12})

    async def test_empty_caption_is_a_visible_revision(self):
        await self.apply('')
        self.assertTrue(self.messages[11].endswith('[empty]'))

    async def test_old_mapping_fetches_current_destination(self):
        self.mapping['targets']['-1007'].pop('source_text')
        await self.apply()
        self.assertTrue(self.messages[11].startswith('OLD GENERAL'))

    async def test_missing_original_never_overwrites(self):
        self.messages = {}
        await self.apply()
        self.assertEqual(self.calls, [])
        self.assertEqual(len(self.replies), 2)
        self.assertEqual(self.replies[1]['message_thread_id'], 6)

    async def test_caption_overflow_is_reply_without_truncation(self):
        self.messages = {11: 'X'*1000, 12: 'Y'*1000}
        await self.apply('Z'*5000)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.messages[11], 'X'*1000)
        self.assertEqual(''.join(c['text'] for c in self.replies[:2]).count('Z'), 5000)
        self.assertTrue(all(edits._units(c['text']) <= 4096 for c in self.replies))

    async def test_astral_unicode_counted_in_utf16(self):
        self.messages = {11: '🧡'*500, 12: '🧡'*500}
        await self.apply('🧡'*30)
        self.assertEqual(self.calls, [])
        self.assertEqual(len(self.replies), 2)
        pieces = list(edits._chunks('🧡'*5000))
        self.assertEqual(''.join(pieces), '🧡'*5000)
        self.assertTrue(all(edits._units(x) <= 4096 for x in pieces))

    async def test_partial_copy_failure_can_retry_without_duplicate(self):
        original = self.bot.edit_message_caption.side_effect
        async def partial(**kw):
            if kw['message_id'] == 12:
                raise ValueError('no permission')
            return await original(**kw)
        self.bot.edit_message_caption.side_effect = partial
        await self.apply()
        self.bot.edit_message_caption.side_effect = original
        await self.apply()
        self.assertEqual(self.messages[11].count('[Edit '), 1)
        self.assertEqual(self.messages[12].count('[Edit '), 1)

    async def test_restart_uses_stored_dedup(self):
        await self.apply()
        edits._history_lock = asyncio.Lock()
        await self.apply()
        self.assertEqual(len(self.calls), 2)

    async def test_out_of_order_event_is_not_appended(self):
        await self.apply('latest')
        await self.apply('stale', info={**self.info, 'edit_date': '2026-11-10 15:38:00+00:00'})
        self.assertNotIn('stale', self.messages[11])

    async def test_concurrent_versions_are_serialized(self):
        await asyncio.gather(self.apply('v2'), self.apply('v3'))
        self.assertIn('v2', self.messages[11])
        self.assertIn('v3', self.messages[11])

    async def test_html_user_input_is_escaped(self):
        await self.apply('<b>not markup</b> & hello')
        self.assertIn('&lt;b&gt;not markup&lt;/b&gt; &amp; hello', self.calls[0]['caption'])
        self.assertEqual(self.calls[0]['parse_mode'], 'HTML')

    async def test_old_link_entities_are_preserved(self):
        from telethon.tl.types import MessageEntityTextUrl
        async def fetch(chat, mid):
            return SimpleNamespace(message='OLD', entities=[MessageEntityTextUrl(0, 3, 'https://example.com')])
        self.fetch = fetch
        await self.apply()
        self.assertIn('href="https://example.com"', self.calls[0]['caption'])

    async def test_ambiguous_reply_is_not_replayed(self):
        self.messages = {}
        self.bot.send_message.side_effect = TimeoutError('unknown outcome')
        await self.apply()
        count = self.bot.send_message.await_count
        await self.apply()
        self.assertEqual(self.bot.send_message.await_count, count)

    async def test_skip_notice_is_text_not_media_caption(self):
        self.mapping['targets']['-1007']['delivery_kind'] = 'file_skip_notice'
        await self.apply()
        self.assertEqual(self.bot.edit_message_text.await_count, 2)

    async def test_mapping_missing_is_safe(self):
        self.mapping = {}
        with patch.object(edits, '_EDIT_MAPPING_RETRIES', 1):
            self.assertFalse(await self.apply())
        self.assertEqual(self.calls, [])
        self.assertEqual(self.replies, [])

    async def test_reply_deduplicates_after_success(self):
        self.messages = {}
        await self.apply()
        await self.apply()
        self.assertEqual(len(self.replies), 2)

    async def test_album_without_group_id_is_safe(self):
        self.assertFalse(await self.apply(kind='album', info={'chat_id': 1, 'msg_id': 2}))

    async def test_one_id_is_not_edited_twice(self):
        self.mapping['targets']['-1007']['topic_message_id'] = 11
        await self.apply()
        self.assertEqual(len(self.calls), 1)

    async def test_network_completed_edit_is_not_appended_twice(self):
        label = '[Edit ' + edits._edit_label(self.info) + ']'
        self.messages[11] += '\n\n' + label + '\nnew'
        await self.apply()
        self.assertEqual(self.messages[11].count('[Edit '), 1)
