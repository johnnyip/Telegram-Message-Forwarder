"""Offline contract tests: no credentials, sessions, brokers or Telegram sockets."""
import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path
import socket
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from telegram.error import BadRequest, NetworkError, RetryAfter
from tg_forwarder.processing import kafka_jobs, job_processing
from tg_forwarder.runtime import bot_runtime
from tg_forwarder.storage import dedup_cache, send_journal


class PipelineContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Fail closed even if a regression accidentally instantiates a real client.
        self.network = patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden"))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.info = {"chat_id": -1001, "msg_id": 17, "sender_id": 9, "chat_title": "offline", "sender_username": "fixture", "sender_display": "Fixture"}
        self.route = {"name": "offline", "targets": [-1002]}
        self.producer = SimpleNamespace(send_and_wait=AsyncMock())
        self.bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=88)))
        self.mapping = patch.object(job_processing, "store_message_mapping", new_callable=AsyncMock)
        self.mapping_mock = self.mapping.start()
        self.addCleanup(self.mapping.stop)

    def context(self):
        async def send_text(route, target, text, info, source_kind):
            return await bot_runtime.send_text_via_bot(
                self.bot, target, text, info, route, source_kind,
                bot_send_semaphore=asyncio.Semaphore(1), log=lambda _: None)
        return SimpleNamespace(
            route_map={"offline": self.route}, ignore_users=set(), ignore_ids=set(),
            should_ignore=lambda *_: False, build_header_from_info=lambda *a, **k: "header",
            text_target_parallel=False, media_target_parallel=False, delete_after_send=False,
            log=lambda _: None, send_text_to_target=send_text,
            send_file_to_target=AsyncMock(return_value=True), is_stale_file=lambda _: False)

    async def publish_text(self):
        await kafka_jobs.publish_text_job(
            SimpleNamespace(text="hello", message="hello"), self.info, [self.route], "new",
            delay_seconds=0, topic="offline-text", producer=self.producer,
            json_bytes=lambda p: json.dumps(p).encode(), log=lambda _: None)
        topic, raw = self.producer.send_and_wait.call_args.args
        self.assertEqual(topic, "offline-text")
        return json.loads(raw)

    async def test_listener_serialized_text_job_reaches_fake_bot(self):
        job = await self.publish_text()
        self.assertEqual(job["route_names"], ["offline"])
        self.assertEqual(job["job_type"], "text")
        self.assertEqual(await job_processing.process_text_job(job, self.context()), [])
        self.bot.send_message.assert_awaited_once_with(
            chat_id=-1002, text="header\nhello", parse_mode="Markdown", message_thread_id=None)
        self.mapping_mock.assert_awaited_once()

    async def test_media_snapshot_job_roundtrip_and_missing_file_error(self):
        path = Path(self.tmp.name) / "sample.bin"
        path.write_bytes(b"offline fixture")
        snapshot = {"path": str(path), "media_type": "document", "caption_text": "caption"}
        with patch.object(kafka_jobs, "snapshot_media_message", AsyncMock(return_value=snapshot)):
            await kafka_jobs.publish_media_job(
                SimpleNamespace(text="caption", message="caption"), self.info, [self.route], "new",
                media_delay_seconds=0, topic="offline-media", producer=self.producer,
                json_bytes=lambda p: json.dumps(p).encode(), log=lambda _: None,
                should_direct_forward_large_media=lambda *_: False,
                snapshot_kwargs={"spool_dir": Path(self.tmp.name), "download_semaphore": None, "run_api": None})
        job = json.loads(self.producer.send_and_wait.call_args.args[1])
        self.assertEqual(job["job_type"], "media_file")
        ctx = self.context()
        await job_processing.process_media_file_job(job, ctx)
        ctx.send_file_to_target.assert_awaited_once()
        job["snapshot"]["path"] = str(Path(self.tmp.name) / "missing.bin")
        with self.assertRaisesRegex(RuntimeError, "media file missing"):
            await job_processing.process_media_file_job(job, ctx)
        self.assertEqual(ctx.send_file_to_target.await_count, 1)

    async def test_all_failed_targets_raise_instead_of_success(self):
        ctx = self.context()
        ctx.send_text_to_target = AsyncMock(return_value=False)
        with self.assertRaisesRegex(RuntimeError, "failed for all targets"):
            await job_processing.process_text_job(await self.publish_text(), ctx)

    async def test_kafka_retry_is_bounded_and_preserves_payload(self):
        self.producer.send_and_wait.side_effect = [OSError("offline"), None]
        with patch.object(kafka_jobs.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            await kafka_jobs.kafka_send("offline", {"job_type": "text"}, self.producer,
                lambda p: json.dumps(p).encode(), lambda _: None, retries=2, retry_delay=2)
        self.assertEqual(self.producer.send_and_wait.await_count, 2)
        self.assertEqual(self.producer.send_and_wait.await_args_list[0], self.producer.send_and_wait.await_args_list[1])
        sleep.assert_awaited_once_with(2)
        self.producer.send_and_wait.reset_mock(side_effect=True)
        self.producer.send_and_wait.side_effect = OSError("offline")
        with patch.object(kafka_jobs.asyncio, "sleep", new_callable=AsyncMock):
            with self.assertRaisesRegex(RuntimeError, "after 2 attempts"):
                await kafka_jobs.kafka_send("offline", {}, self.producer, lambda _: b"{}", lambda _: None, retries=2)
        self.assertEqual(self.producer.send_and_wait.await_count, 2)

    async def test_bot_retry_after_int_timedelta_and_network_error(self):
        for error, delay in [(RetryAfter(2), 3), (RetryAfter(timedelta(seconds=2)), 3), (NetworkError("offline"), 3)]:
            with self.subTest(error=type(error).__name__), patch.dict(os.environ, {"PTB_TIMEDELTA": "true"}):
                send = AsyncMock(side_effect=[error, "ok"])
                with patch.object(bot_runtime.asyncio, "sleep", new_callable=AsyncMock) as sleep:
                    self.assertEqual(await bot_runtime._call_with_retry(send), "ok")
                sleep.assert_awaited_once_with(delay)
                self.assertEqual(send.await_count, 2)
        timedelta_error = RuntimeError("offline rate limit")
        timedelta_error.retry_after = timedelta(seconds=4)
        with patch.object(bot_runtime.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            self.assertEqual(await bot_runtime._call_with_retry(AsyncMock(side_effect=[timedelta_error, "ok"])), "ok")
        sleep.assert_awaited_once_with(5)
        send = AsyncMock(side_effect=BadRequest("invalid"))
        with self.assertRaises(BadRequest):
            await bot_runtime._call_with_retry(send)
        self.assertEqual(send.await_count, 1)
        send = AsyncMock(side_effect=NetworkError("offline"))
        with patch.object(bot_runtime.asyncio, "sleep", new_callable=AsyncMock):
            with self.assertRaises(NetworkError):
                await bot_runtime._call_with_retry(send)
        self.assertEqual(send.await_count, 2)

    async def test_dedup_and_persistent_journal_ttl_and_failed_retry(self):
        with patch.object(dedup_cache, "DEDUP_CACHE", {}), patch.object(dedup_cache, "_now", return_value=100) as clock:
            self.assertFalse(dedup_cache.dedup_seen("fixture"))
            dedup_cache.dedup_mark("fixture")
            self.assertTrue(dedup_cache.dedup_seen("fixture"))
            clock.return_value = 101 + dedup_cache.DEDUP_TTL_SECONDS
            self.assertFalse(dedup_cache.dedup_seen("fixture"))
        with patch.object(send_journal, "SEND_JOURNAL_PATH", Path(self.tmp.name) / "journal.json"), patch.object(send_journal, "_now", return_value=100) as clock:
            self.assertTrue(send_journal.journal_claim("fixture"))
            self.assertFalse(send_journal.journal_claim("fixture"))
            self.assertEqual(send_journal.journal_record_failure("fixture"), 1)
            self.assertTrue(send_journal.journal_claim("fixture"))
            send_journal.journal_mark("fixture", {"status": "done"})
            self.assertFalse(send_journal.journal_claim("fixture"))
            clock.return_value = 101 + send_journal.SEND_JOURNAL_TTL_SECONDS
            self.assertTrue(send_journal.journal_claim("fixture"))
