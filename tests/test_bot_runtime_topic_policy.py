import asyncio
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace

redis_module = types.ModuleType("redis")
redis_asyncio_module = types.ModuleType("redis.asyncio")
redis_asyncio_module.Redis = object
redis_module.asyncio = redis_asyncio_module
sys.modules.setdefault("redis", redis_module)
sys.modules.setdefault("redis.asyncio", redis_asyncio_module)

import tg_forwarder.runtime.bot_runtime as mod


class BotRuntimeTopicPolicyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._orig_resolve = mod.resolve_topic_thread_id
        self._orig_bot_send_text = mod.bot_send_text
        self._orig_bot_send_file = mod.bot_send_file
        self._orig_delivery_mode = mod.FORUM_TOPIC_DELIVERY_MODE

    def tearDown(self):
        mod.resolve_topic_thread_id = self._orig_resolve
        mod.bot_send_text = self._orig_bot_send_text
        mod.bot_send_file = self._orig_bot_send_file
        mod.FORUM_TOPIC_DELIVERY_MODE = self._orig_delivery_mode

    async def test_send_text_goes_general_only_without_topic_lookup(self):
        calls = []

        async def fake_send_text(bot, target, text, message_thread_id=None):
            calls.append(message_thread_id)
            return SimpleNamespace(message_id=123)

        async def fail_resolve(*args, **kwargs):
            raise AssertionError("resolve_topic_thread_id should not be called for plain text")

        mod.bot_send_text = fake_send_text
        mod.resolve_topic_thread_id = fail_resolve
        mod.FORUM_TOPIC_DELIVERY_MODE = "mirror"

        outcome = await mod.send_text_via_bot(
            bot=object(),
            target=-100123,
            combined="hello",
            info={"msg_id": 1, "chat_id": 2, "chat_title": "src", "sender_id": 3, "sender_username": "alice", "sender_display": "Alice"},
            route={"name": "test"},
            source_kind="new",
            bot_send_semaphore=asyncio.Semaphore(1),
            log=lambda payload: None,
        )

        self.assertTrue(outcome.ok)
        self.assertEqual(calls, [None])
        self.assertIsNone(outcome.message_thread_id)
        self.assertIsNone(outcome.topic_message_id)

    async def test_send_text_topic_only_uses_individual_topic_without_general_copy(self):
        calls = []

        async def fake_send_text(bot, target, text, message_thread_id=None):
            calls.append(message_thread_id)
            return SimpleNamespace(message_id=789)

        async def fake_resolve(*args, **kwargs):
            return 333

        mod.bot_send_text = fake_send_text
        mod.resolve_topic_thread_id = fake_resolve
        mod.FORUM_TOPIC_DELIVERY_MODE = "topic_only"

        outcome = await mod.send_text_via_bot(
            bot=object(),
            target=-100123,
            combined="hello",
            info={"msg_id": 1, "chat_id": 2, "chat_title": "src", "sender_id": 3, "sender_username": "alice", "sender_display": "Alice"},
            route={"name": "test"},
            source_kind="new",
            bot_send_semaphore=asyncio.Semaphore(1),
            log=lambda payload: None,
        )

        self.assertTrue(outcome.ok)
        self.assertEqual(calls, [333])
        self.assertEqual(outcome.message_thread_id, 333)
        self.assertEqual(outcome.topic_message_id, 789)
        self.assertIsNone(outcome.general_message_id)

    async def test_oversized_file_notice_stays_general_only_and_skips_topic_lookup(self):
        calls = []

        async def fake_send_text(bot, target, text, message_thread_id=None):
            calls.append(message_thread_id)
            return SimpleNamespace(message_id=456)

        async def fail_resolve(*args, **kwargs):
            raise AssertionError("resolve_topic_thread_id should not be called for oversized-file notice")

        mod.bot_send_text = fake_send_text
        mod.resolve_topic_thread_id = fail_resolve

        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = Path(tmpdir) / "big.bin"
            fpath.write_bytes(b"1234567890")
            outcome = await mod.send_file_via_bot(
                bot=object(),
                target=-100123,
                fpath=fpath,
                caption="cap",
                info={"msg_id": 1, "chat_id": 2, "chat_title": "src", "sender_id": 3, "sender_username": "alice", "sender_display": "Alice", "_media_type": "photo"},
                route={"name": "test"},
                source_kind="new",
                bot_send_semaphore=asyncio.Semaphore(1),
                log=lambda payload: None,
                upload_max_bytes=1,
            )

        self.assertTrue(outcome.ok)
        self.assertTrue(outcome.preserve_local_copy)
        self.assertEqual(calls, [None])
        self.assertIsNone(outcome.message_thread_id)
        self.assertIsNone(outcome.topic_message_id)

    async def test_send_file_topic_only_skips_general_copy_when_topic_available(self):
        calls = []

        async def fake_send_file(bot, target, file_path, caption="", media_type=None, message_thread_id=None, log_fn=None):
            calls.append(message_thread_id)
            return SimpleNamespace(message_id=654)

        async def fake_resolve(*args, **kwargs):
            return 444

        mod.bot_send_file = fake_send_file
        mod.resolve_topic_thread_id = fake_resolve
        mod.FORUM_TOPIC_DELIVERY_MODE = "topic_only"

        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = Path(tmpdir) / "pic.jpg"
            fpath.write_bytes(b"1234")
            outcome = await mod.send_file_via_bot(
                bot=object(),
                target=-100123,
                fpath=fpath,
                caption="cap",
                info={"msg_id": 1, "chat_id": 2, "chat_title": "src", "sender_id": 3, "sender_username": "alice", "sender_display": "Alice", "_media_type": "photo"},
                route={"name": "test"},
                source_kind="new",
                bot_send_semaphore=asyncio.Semaphore(1),
                log=lambda payload: None,
                upload_max_bytes=999999,
            )

        self.assertTrue(outcome.ok)
        self.assertEqual(calls, [444])
        self.assertEqual(outcome.message_thread_id, 444)
        self.assertEqual(outcome.topic_message_id, 654)
        self.assertIsNone(outcome.general_message_id)


if __name__ == "__main__":
    unittest.main()
