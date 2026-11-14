import copy
import fcntl
import hashlib
import json
import os
import unittest
from unittest.mock import patch
import test_inbound_photos as inbound
from tg_forwarder.control.recover_inbound import recover


class RetryFailedInboundTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await inbound.InboundTests.asyncSetUp(self)
        self.engine.bot_id=inbound.BOT
    asyncTearDown=inbound.InboundTests.asyncTearDown
    queue=inbound.InboundTests.queue

    async def fixture(self,mid=89900):
        raw=inbound.raw_update(mid);self.store.ingest(raw)
        self.store.db.execute("UPDATE updates SET state='done' WHERE id=?",(raw['update_id'],))
        await self.queue(raw);task=self.store.claim()
        self.store.set_task(task['id'],state='failed',error='old missing reuse')
        self.store.db.execute("UPDATE inbound_notices SET reply=73337,state='sent'")
        text=self.store.db.execute('SELECT payload FROM updates WHERE id=?',(raw['update_id'],)).fetchone()[0]
        return {'version':1,'owner_uid':inbound.OWNER,'bot_id':inbound.BOT,'db':str(self.config.db),'updates':[{
            'update_id':raw['update_id'],'payload_sha256':hashlib.sha256(text.encode()).hexdigest(),
            'task_id':task['id'],'task_payload_sha256':hashlib.sha256(task['payload'].encode()).hexdigest(),'expected_attempt':1}]}

    async def test_exact_retry_preserves_ids_updates_offset_payload_and_album_anchor(self):
        plan=await self.fixture();tid=plan['updates'][0]['task_id']
        before_updates=[tuple(r) for r in self.store.db.execute('SELECT * FROM updates')]
        before_offset=self.store.get_meta('offset');before_payload=self.store.task(tid,inbound.OWNER,inbound.OWNER)['payload']
        self.assertEqual(recover(self.config,plan,retry_failed=True)['mode'],'dry-run-retry-failed')
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':str(inbound.BOT)+':offline'}):
            result=recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)
            self.assertEqual(result['task_ids'],[tid]);self.assertEqual(result['mode'],'retry-failed-only')
            queued=self.store.task(tid,inbound.OWNER,inbound.OWNER);self.assertEqual(queued['attempt'],1)
            row=self.store.claim();self.assertEqual(row['id'],tid);self.assertEqual(row['attempt'],2)
            with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)
        self.assertEqual(self.store.task(tid,inbound.OWNER,inbound.OWNER)['state'],'running')
        self.assertEqual(self.store.task(tid,inbound.OWNER,inbound.OWNER)['payload'],before_payload)
        self.assertEqual([tuple(r) for r in self.store.db.execute('SELECT * FROM updates')],before_updates)
        self.assertEqual(self.store.get_meta('offset'),before_offset)
        self.assertEqual(self.store.db.execute('SELECT reply FROM inbound_notices').fetchone()[0],73337)
        self.bot.send_message.assert_not_awaited()

    async def test_identical_plan_cannot_retry_again_after_worker_claims_and_fails_attempt_two(self):
        plan=await self.fixture();tid=plan['updates'][0]['task_id']
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':str(inbound.BOT)+':offline'}):
            recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)
            queued=self.store.task(tid,inbound.OWNER,inbound.OWNER)
            self.assertEqual(queued['state'],'queued');self.assertEqual(queued['attempt'],1)
            with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)
            claimed=self.store.claim();self.assertEqual(claimed['attempt'],2)
            with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)
            self.store.set_task(tid,state='failed',error='second failure')
            before=tuple(self.store.db.execute('SELECT * FROM tasks WHERE id=?',(tid,)).fetchone())
            with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)
            self.assertEqual(tuple(self.store.db.execute('SELECT * FROM tasks WHERE id=?',(tid,)).fetchone()),before)
            self.store.set_task(tid,state='done')
            with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)

    async def test_done_ambiguous_running_queued_cancelled_states_refused(self):
        plan=await self.fixture();tid=plan['updates'][0]['task_id']
        for state in ('done','ambiguous','running','queued','cancelled'):
            self.store.set_task(tid,state=state)
            with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True)

    async def test_wrong_owner_bot_attempt_payload_updatehash_and_taskkey_refused(self):
        plan=await self.fixture()
        for field,value in (('expected_attempt',2),('task_payload_sha256','0'*64),('payload_sha256','0'*64),('task_id','unknown')):
            changed=copy.deepcopy(plan);changed['updates'][0][field]=value
            with self.assertRaises(ValueError):recover(self.config,changed,retry_failed=True)
        for field,value in (('owner_uid',192114587),('bot_id',inbound.BOT+1)):
            changed=copy.deepcopy(plan);changed[field]=value
            with self.assertRaises(ValueError):recover(self.config,changed,retry_failed=True)
        self.store.db.execute("UPDATE tasks SET key='wrong-key'")
        with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True)

    async def test_batch_validation_is_atomic_no_first_row_requeued(self):
        first=await self.fixture(89900);second=await self.fixture(89901)
        first['updates']+=second['updates'];first['updates'][1]['task_payload_sha256']='0'*64
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':str(inbound.BOT)+':offline'}):
            with self.assertRaises(ValueError):recover(self.config,first,retry_failed=True,apply=True,quiescent=True)
        self.assertEqual({r[0] for r in self.store.db.execute('SELECT state FROM tasks')},{'failed'})

    async def test_apply_requires_quiescence_correct_token_and_state_lock(self):
        plan=await self.fixture()
        with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True,apply=True)
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':'123:offline'}):
            with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)
        with open(str(self.config.db)+'.lock','a') as lock,patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':str(inbound.BOT)+':offline'}):
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaises(ValueError):recover(self.config,plan,retry_failed=True,apply=True,quiescent=True)
