import asyncio,fcntl,json,os,tempfile,time,unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock,patch
from telegram.error import RetryAfter,BadRequest,TimedOut
from tg_forwarder.spool_lifecycle import Spool,configure,manager,target_send
from tg_forwarder.runtime.bot_runtime import _call_with_retry,DEFER_MEDIA_RETRIES
from tg_forwarder.shared_media import definite_rejection

class DurableQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.m=Spool(self.tmp.name)
        self.r=NS(topic='media',partition=0,offset=1,value=b'{"info":{"chat_id":1}}')
    def test_handoff_survives_reopen_and_is_idempotent(self):
        self.assertTrue(self.m.handoff(self.r,'unknown'))
        self.assertTrue(Spool(self.tmp.name).handoff(self.r,'unknown'))
        with self.m.db() as c:
            rows=c.execute('SELECT * FROM consumer_handoffs').fetchall()
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['payload'],self.r.value.decode())
    def test_payload_mutation_never_acknowledged(self):
        self.m.handoff(self.r,'unknown');self.r.value=b'{}'
        with self.assertRaises(ValueError):self.m.handoff(self.r,'unknown')
    def test_retry_claim_is_exclusive_and_crash_does_not_replay(self):
        self.m.retry_handoff(self.r,0)
        with self.m.db() as c:c.execute('UPDATE retry_queue SET due=0')
        r=self.m.claim_retry();self.assertEqual(r.value,self.r.value)
        self.assertIsNone(Spool(self.tmp.name).claim_retry())
        self.assertEqual(len(self.m.retry_records()),1)
    def test_retry_budget_and_terminal_states(self):
        self.m.retry_handoff(self.r,0)
        for _ in range(3):
            with self.m.db() as c:c.execute('UPDATE retry_queue SET due=0')
            r=self.m.claim_retry();self.assertIsNotNone(r)
            if _<2:self.assertTrue(self.m.retry_handoff(r,0))
        self.assertFalse(self.m.retry_handoff(self.r,0));self.m.finish_retry(self.r,False)
        self.assertEqual(self.m.retry_records(),[])
    def test_reschedule_not_overwritten_by_finish(self):
        self.m.retry_handoff(self.r,0)
        with self.m.db() as c:c.execute('UPDATE retry_queue SET due=0')
        r=self.m.claim_retry();self.m.retry_handoff(r,5);self.m.finish_retry(r,False)
        self.assertEqual(self.m.handoff_summary()['retry_states'],{'pending':1})
    def test_resolved_retry_is_not_a_reconciliation_failure(self):
        self.m.retry_handoff(self.r,0)
        with self.m.db() as c:c.execute('UPDATE retry_queue SET due=0')
        r=self.m.claim_retry();self.m.finish_retry(r,True)
        summary=self.m.handoff_summary();self.assertEqual(summary['pending_reconciliation'],0);self.assertEqual(summary['retry_states'],{'resolved':1})
    def test_indexes_and_schema_reused(self):
        with self.m.db() as c:
            names={r[1] for r in c.execute("PRAGMA index_list('files')")}
        self.assertIn('spool_files_state_due',names)
        identity=self.m._schema_identity
        with self.m.db():pass
        self.assertEqual(identity,self.m._schema_identity);self.assertEqual(self.m.db_metrics['transactions'],2)
    def test_badrequest_backend_error_is_uncertain(self):
        self.assertFalse(definite_rejection(BadRequest('Internal server error during file upload')))
        self.assertFalse(definite_rejection(TimedOut()))
        self.assertTrue(definite_rejection(BadRequest('Message thread not found')))
        self.assertTrue(definite_rejection(RetryAfter(5)))

class RetryAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_deferred_retry_does_not_sleep_or_send_twice(self):
        send=AsyncMock(side_effect=RetryAfter(300));token=DEFER_MEDIA_RETRIES.set(True)
        try:
            with patch('tg_forwarder.runtime.bot_runtime.asyncio.sleep',new_callable=AsyncMock) as sleep:
                with self.assertRaises(RetryAfter):await _call_with_retry(send)
                sleep.assert_not_awaited();self.assertEqual(send.await_count,1)
        finally:DEFER_MEDIA_RETRIES.reset(token)
    async def test_timeout_is_never_retryable(self):
        send=AsyncMock(side_effect=TimedOut())
        with self.assertRaises(TimedOut):await _call_with_retry(send)
        self.assertEqual(send.await_count,1)

