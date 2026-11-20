import asyncio,json,time,unittest
from types import SimpleNamespace as NS
from aiokafka.structs import TopicPartition
from tg_forwarder.processing.fair_scheduler import ContiguousOffsets,FairScheduler,describe
from tg_forwarder.processing.media_runner import ConcurrentCommitBatcher,MediaRunner

def record(offset,source=1,sender=None,size=1,due=0):
    return NS(topic='media',partition=0,offset=offset,value=json.dumps({'job_type':'media_file','info':{'chat_id':source,'sender_id':sender if sender is not None else offset},'spool_targets':[['route',-100]],'snapshot':{'media_size':size},'due_at':due}).encode())

class OffsetTests(unittest.TestCase):
    def test_out_of_order_never_crosses_gap(self):
        t=ContiguousOffsets()
        for n in (10,11,13):t.register('p',n)
        t.complete('p',13);self.assertEqual(t.ready,{})
        t.complete('p',10);self.assertEqual(t.ready,{'p':11})
        t.complete('p',11);self.assertEqual(t.ready,{'p':14})
    def test_partition_independence_and_revoke(self):
        t=ContiguousOffsets();t.register('p',1);t.register('q',8);t.complete('q',8)
        self.assertEqual(t.ready,{'q':9});t.forget(['p']);self.assertNotIn('p',t.pending)
    def test_unregistered_completion_rejected(self):
        with self.assertRaises(ValueError):ContiguousOffsets().complete('p',1)
    def test_registration_order(self):
        t=ContiguousOffsets();t.register('p',2)
        with self.assertRaises(ValueError):t.register('p',1)
    def test_size_classification_and_cross_source_sender_keys(self):
        a=describe(record(1,1,7,size=25*1024*1024));b=describe(record(2,2,7))
        self.assertEqual(a.lane,'slow');self.assertEqual(b.lane,'fast');self.assertEqual(a.keys,b.keys)

class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_parallelism_bounded_and_same_sender_ordered(self):
        active=0;peak=0;seen=[]
        async def run(r):
            nonlocal active,peak
            active+=1;peak=max(peak,active);seen.append(r.offset)
            await asyncio.sleep(.015);active-=1;return True
        s=FairScheduler(run,workers=3,capacity=12)
        for n in range(9):await s.submit(record(n,sender=n%3))
        await s.close()
        self.assertEqual(peak,3)
        for sender in range(3):self.assertEqual([n for n in seen if n%3==sender],list(range(sender,9,3)))
        self.assertEqual(s.counts['completed'],9);self.assertEqual(s.total,0)
    async def test_slow_lane_does_not_take_fast_slots(self):
        slow=asyncio.Event();fast=asyncio.Event();started=[]
        async def run(r):
            started.append(r.offset)
            if r.offset<3:await slow.wait()
            else:fast.set()
            return True
        s=FairScheduler(run,workers=3,capacity=8,slow_workers=1)
        for n in range(3):await s.submit(record(n,size=30*1024*1024))
        await s.submit(record(3));await asyncio.wait_for(fast.wait(),1)
        self.assertEqual([n for n in started if n<3],[0]);slow.set();await s.close()
    async def test_source_fairness(self):
        seen=[]
        async def run(r):seen.append(r.offset);return True
        s=FairScheduler(run,workers=1,capacity=30)
        for n in range(10):await s.submit(record(n,source=1))
        await s.submit(record(10,source=2));await s.close()
        self.assertLess(seen.index(10),3)
    async def test_due_job_does_not_block_other_flow(self):
        seen=[]
        async def run(r):seen.append(r.offset);return True
        s=FairScheduler(run,workers=2,capacity=4)
        await s.submit(record(0,due=time.time()+60));await s.submit(record(1))
        await asyncio.sleep(.02);self.assertEqual(seen,[1]);await s.close(cancel=True)
        self.assertEqual(s.total,0)
    async def test_delayed_head_preserves_same_flow_order(self):
        seen=[]
        async def run(r):seen.append(r.offset);return True
        s=FairScheduler(run,workers=2,capacity=4)
        await s.submit(record(0,sender=7,due=time.time()+.03));await s.submit(record(1,sender=7))
        await s.close();self.assertEqual(seen,[0,1])
    async def test_unknown_not_retried_and_does_not_block_other_source(self):
        calls=[]
        async def run(r):calls.append(r.offset);return False if r.offset==0 else True
        s=FairScheduler(run,workers=2,capacity=4)
        await s.submit(record(0));await s.submit(record(1,source=2));await s.close()
        self.assertEqual(sorted(calls),[0,1]);self.assertEqual(s.counts['unconfirmed'],1)
    async def test_cancel_owns_started_tasks_and_discards_unstarted(self):
        exited=asyncio.Event();started=asyncio.Event()
        async def run(r):
            started.set()
            try:await asyncio.Event().wait()
            finally:exited.set()
        s=FairScheduler(run,workers=1,capacity=4)
        await s.submit(record(0));await s.submit(record(1));await started.wait()
        await s.close(cancel=True);self.assertTrue(exited.is_set());self.assertEqual(s.total,0)
    async def test_concurrent_commit_does_not_erase_new_completion(self):
        entered=asyncio.Event();release=asyncio.Event();commits=[]
        async def commit(offsets):
            commits.append(offsets);entered.set();await release.wait()
        c=NS(commit=commit);b=ConcurrentCommitBatcher(c,'test',every=1)
        a=record(0);z=record(1);b.register(a);b.register(z);b.mark_processed(a)
        task=asyncio.create_task(b.maybe_commit(force=True));await entered.wait();b.mark_processed(z);release.set();await task
        tp=TopicPartition('media',0);self.assertEqual(b.tracker.ready,{tp:2})
        await b.maybe_commit(force=True);self.assertEqual(commits,[{tp:1},{tp:2}])
    async def test_failed_commit_retains_ready(self):
        async def fail(_):raise OSError('broker unavailable')
        b=ConcurrentCommitBatcher(NS(commit=fail),'test');r=record(0);b.register(r);b.mark_processed(r)
        with self.assertRaises(OSError):await b.maybe_commit(force=True)
        self.assertEqual(b.tracker.ready,{TopicPartition('media',0):1})
    async def test_revoke_drains_before_commit_and_forgets(self):
        order=[]
        async def commit(_):order.append('commit')
        runner=MediaRunner(NS(commit=commit),'media',None)
        async def close(**_):order.append('drain')
        runner.scheduler=NS(close=close);r=record(0);runner.batch.register(r);runner.batch.mark_processed(r)
        await runner.on_partitions_revoked({TopicPartition('media',0)})
        self.assertEqual(order,['drain','commit']);self.assertFalse(runner.batch.tracker.pending)


class RunnerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_poll_dispatch_commit_and_shutdown(self):
        from unittest.mock import AsyncMock
        tp=TopicPartition('media',0);finished=asyncio.Event();seen=[];active=0;peak=0
        class Consumer:
            def __init__(self):self.items=[record(i,source=i%2) for i in range(12)];self.commits=[];self.paused=False
            def subscribe(self,topics,listener):self.listener=listener
            async def start(self):await self.listener.on_partitions_assigned({tp})
            def assignment(self):return {tp}
            def pause(self,*args):self.paused=True
            def resume(self,*args):self.paused=False
            async def getmany(self,**kw):
                await asyncio.sleep(.002)
                if self.paused:return {}
                count=kw['max_records'];items=self.items[:count];self.items=self.items[count:]
                return {tp:items} if items else {}
            async def commit(self,offsets):self.commits.append(dict(offsets))
            async def stop(self):pass
        c=Consumer()
        async def handler(r,b):
            nonlocal active,peak
            active+=1;peak=max(peak,active);await asyncio.sleep(.01 if r.offset%2 else .02)
            active-=1;seen.append(r.offset);b.mark_processed(r)
            if len(seen)==12:finished.set()
            return True
        runner=MediaRunner(c,'media',handler,workers=3,capacity=6,every=1)
        runner.refresh_retry_keys=AsyncMock();runner.metrics=AsyncMock()
        runner.retry_loop=lambda:asyncio.Event().wait()
        task=asyncio.create_task(runner.run())
        try:await asyncio.wait_for(finished.wait(),2)
        finally:
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)
        self.assertEqual(sorted(seen),list(range(12)));self.assertEqual(peak,3)
        self.assertEqual(c.commits[-1],{tp:12})
        self.assertEqual([x[tp] for x in c.commits],sorted(x[tp] for x in c.commits))
    async def test_blocked_retry_flow_preserves_other_sources(self):
        seen=[]
        async def handler(r):seen.append(r.offset);return True
        s=FairScheduler(handler,workers=3,capacity=6)
        blocked=record(0,sender=7);s.blocked_keys.update(describe(blocked).keys)
        await s.submit(blocked);await s.submit(record(1,source=2,sender=8));await asyncio.sleep(.02)
        self.assertEqual(seen,[1]);s.blocked_keys.clear();s.changed.set();await s.close();self.assertEqual(seen,[1,0])
    async def test_capacity_backpressure_never_creates_unbounded_tasks(self):
        gate=asyncio.Event()
        async def handler(r):await gate.wait();return True
        s=FairScheduler(handler,workers=2,capacity=2)
        await s.submit(record(0));await s.submit(record(1))
        third=asyncio.create_task(s.submit(record(2)));await asyncio.sleep(.02)
        self.assertFalse(third.done());self.assertEqual(s.total,2)
        gate.set();await third;await s.close();self.assertEqual(s.counts['completed'],3)
    def test_legacy_forward_and_owned_snapshot_share_topic_flow(self):
        a=record(0,sender=7);job=json.loads(a.value);job.pop('spool_targets');job['route_names']=['r'];job['job_type']='media_forward';a.value=json.dumps(job).encode()
        self.assertEqual(describe(a,route_map={'r':{'targets':[-100]}}).keys,describe(record(1,sender=7)).keys)

