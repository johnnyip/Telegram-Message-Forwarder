import ast,asyncio,fcntl,json,os,tempfile,time,unittest
from pathlib import Path
from types import SimpleNamespace as NS
from typing import List
from unittest.mock import AsyncMock,Mock,patch
from telegram.error import RetryAfter,TimedOut
from tg_forwarder import spool_lifecycle as sl

class NativeSafetyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'spool';sl.configure(self.root);self.m=sl.manager()
        self.addCleanup(sl.configure,Path(self.tmp.name)/'unused')
        self.addCleanup(sl._MANAGERS.pop,self.root,None)
        self.addCleanup(lambda:[self.m.release(t) for t in list(self.m.leases)])
    def job(self):
        token,fd,p=self.m.allocate('new',1,'photo.jpg');os.write(fd,b'data');os.close(fd)
        p=self.m.finalize(token,True)
        job={'info':{'chat_id':1,'msg_id':1},'source_kind':'new','route_names':['r'],'snapshot':{'path':str(p),'spool_token':token},'spool_targets':[['r',2]]}
        self.m.bind(job,job['spool_targets']);self.m.start(job,job['spool_targets'],True)
        return job
    async def test_only_definite_429_releases_target_for_retry(self):
        job=self.job();send=AsyncMock(side_effect=RetryAfter(1))
        with self.assertRaises(RetryAfter):await sl.target_send(job,'r',2,send)
        send.side_effect=None;send.return_value=NS(ok=True,sent_message_id=3,delivery_kind='file')
        result=await sl.target_send(job,'r',2,send)
        self.assertEqual(result.sent_message_id,3);self.assertTrue(self.m.complete(job));self.assertEqual(send.await_count,2)
    async def test_timeout_retains_target_claim_and_never_resends(self):
        job=self.job();send=AsyncMock(side_effect=TimedOut())
        with self.assertRaises(TimedOut):await sl.target_send(job,'r',2,send)
        with self.assertRaises(RuntimeError):await sl.target_send(job,'r',2,send)
        self.assertEqual(send.await_count,1)
    def test_cleanup_filesystem_work_does_not_hold_database_lock(self):
        job=self.job();self.m.begin_target(job,'r',2);self.m.target_result(job,'r',2,NS(sent_message_id=3,delivery_kind='file'))
        self.m.finish(job,[job['snapshot']['path']],0)
        original=self.m.claim_remove;checks=[]
        def remove(*a,**kw):
            fd=os.open(self.m.guard()/'db.lock',os.O_RDWR)
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);checks.append(True)
            finally:os.close(fd)
            return original(*a,**kw)
        with patch.object(self.m,'claim_remove',side_effect=remove):result=self.m.scan()
        self.assertEqual(result['deleted'],1);self.assertEqual(checks,[True]);self.assertTrue(self.m.complete(job))
    def test_abandoned_filesystem_work_does_not_hold_database_lock(self):
        token,fd,p=self.m.allocate('new',1,'old.jpg');os.write(fd,b'data');os.close(fd);self.m.finalize(token,True);self.m.release(token)
        original=self.m.claim_remove;checks=[]
        def remove(*a,**kw):
            fd=os.open(self.m.guard()/'db.lock',os.O_RDWR)
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);checks.append(True)
            finally:os.close(fd)
            return original(*a,**kw)
        with patch.object(self.m,'claim_remove',side_effect=remove):result=self.m.scan(now=time.time()+74*3600)
        self.assertEqual(result['abandoned'],1);self.assertEqual(checks,[True])

class HandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        node=next(n for n in ast.parse(Path('main.py').read_text()).body if isinstance(n,ast.AsyncFunctionDef) and n.name=='process_parallel_media_record')
        self.env=dict(List=List,json=json,maybe_debug_log=Mock(),log=Mock(),tstamp=lambda:'now',sleep_until_due=AsyncMock(),future_comment_job_permitted=AsyncMock(return_value=True),job_fingerprint=lambda _:'fingerprint',journal_get=Mock(return_value=None),journal_claim=Mock(return_value=True),journal_mark=Mock(),journal_record_failure=Mock(return_value=1),dispatch_media_job=AsyncMock(return_value=[]),build_job_processing_ctx=lambda:None,DELETE_AFTER_SEND=True)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),self.env)
        self.record=NS(topic='media',partition=0,offset=5,value=json.dumps({'job_type':'media_file','info':{'chat_id':1,'msg_id':2},'route_names':['r']}).encode())
        self.batch=NS(mark_processed=Mock(),maybe_commit=AsyncMock())
        self.mocks={}
        for name in ('classify','completed','finish','durable_failure_handoff','definite_retry_handoff'):
            m=AsyncMock(return_value=False if name in ('completed','definite_retry_handoff') else True)
            guard=patch.object(sl,name,m);guard.start();self.addCleanup(guard.stop);self.mocks[name]=m
        guard=patch.object(sl,'owned',return_value=False);guard.start();self.addCleanup(guard.stop)
    async def test_success_is_acknowledged(self):
        self.assertTrue(await self.env['process_parallel_media_record'](self.record,self.batch));self.batch.mark_processed.assert_called_once_with(self.record)
    async def test_unknown_requires_durable_handoff_before_ack(self):
        self.env['dispatch_media_job'].side_effect=TimedOut()
        self.assertFalse(await self.env['process_parallel_media_record'](self.record,self.batch))
        self.mocks['durable_failure_handoff'].assert_awaited_once();self.batch.mark_processed.assert_called_once()
    async def test_failed_handoff_keeps_offset_unacknowledged(self):
        self.env['dispatch_media_job'].side_effect=TimedOut();self.mocks['durable_failure_handoff'].side_effect=OSError('disk full')
        with self.assertRaises(OSError):await self.env['process_parallel_media_record'](self.record,self.batch)
        self.batch.mark_processed.assert_not_called()
    async def test_unproven_journal_never_silently_skips_or_resends(self):
        self.env['journal_get'].return_value={'status':'processing'}
        self.assertFalse(await self.env['process_parallel_media_record'](self.record,self.batch))
        self.env['dispatch_media_job'].assert_not_awaited();self.mocks['durable_failure_handoff'].assert_awaited_once()
    async def test_429_uses_retry_handoff_not_unknown_queue(self):
        self.env['dispatch_media_job'].side_effect=RetryAfter(100);self.mocks['definite_retry_handoff'].return_value=True
        self.assertFalse(await self.env['process_parallel_media_record'](self.record,self.batch));self.batch.mark_processed.assert_called_once()
        self.mocks['durable_failure_handoff'].assert_not_awaited()

