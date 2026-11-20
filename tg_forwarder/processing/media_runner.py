"""Kafka ownership adapter for bounded media workers."""
import asyncio
import time
from functools import partial
from aiokafka import ConsumerRebalanceListener
from aiokafka.structs import TopicPartition
from .fair_scheduler import ContiguousOffsets, FairScheduler, describe

class ConcurrentCommitBatcher:
    def __init__(self, consumer, name, *, every=20, interval=5, log=lambda _:None):
        self.consumer=consumer
        self.name=name
        self.tracker=ContiguousOffsets()
        self.every=every
        self.interval=interval
        self.log=log
        self.pending_records=0
        self.last_commit=time.monotonic()
        self.lock=asyncio.Lock()
        self.changed=asyncio.Event()

    def register(self, record):
        self.tracker.register(TopicPartition(record.topic,record.partition),record.offset)

    def mark_processed(self, record):
        self.tracker.complete(TopicPartition(record.topic,record.partition),record.offset)
        self.pending_records+=1
        self.changed.set()

    async def maybe_commit(self, *, force=False, reason="periodic"):
        async with self.lock:
            if not self.tracker.ready:
                return
            if not force and self.pending_records<self.every and time.monotonic()-self.last_commit<self.interval:
                return
            offsets=dict(self.tracker.ready)
            await self.consumer.commit(offsets)
            # Workers can finish while commit awaits the broker. Never erase a
            # newer high-water mark, and never commit over unfinished records.
            for tp,offset in offsets.items():
                if self.tracker.ready.get(tp)==offset:
                    del self.tracker.ready[tp]
            self.log({"type":"perf","op":"media_offset_commit","lane":self.name,
                      "reason":reason,"offsets":{str(k):v for k,v in offsets.items()}})
            self.pending_records=0
            self.last_commit=time.monotonic()
            self.changed.set()

    def outstanding(self):
        return sum(len(q) for q in self.tracker.pending.values())


class MediaRunner(ConsumerRebalanceListener):
    def __init__(self, consumer, topic, handler, *, workers=3, capacity=96,
                 slow_workers=1, log=lambda _:None, name="media", every=20, interval=5, route_map=None):
        self.consumer=consumer
        self.topic=topic
        self.handler=handler
        self.describe_work=partial(describe,route_map=route_map)
        self.log=log
        self.name=name
        self.workers=workers
        self.capacity=capacity
        self.slow_workers=slow_workers
        self.batch=ConcurrentCommitBatcher(consumer,name,every=every,interval=interval,log=log)
        self.scheduler=None
        self.generation=0
        self.stopping=False
        self.last_metrics=0
        self.paused=set()
        self.retry_keys=set()
        self.retry_refresh_lock=asyncio.Lock()

    def new_scheduler(self):
        self.scheduler=FairScheduler(self.handle,workers=self.workers,capacity=self.capacity,
                                     slow_workers=self.slow_workers,describe_work=self.describe_work,log=self.log,name=self.name)
        self.scheduler.blocked_keys=self.retry_keys

    async def handle(self, record):
        from ..runtime.bot_runtime import DEFER_MEDIA_RETRIES
        token=DEFER_MEDIA_RETRIES.set(True)
        try:
            result=await self.handler(record,self.batch)
            if not result:await self.refresh_retry_keys()
            return result
        finally:DEFER_MEDIA_RETRIES.reset(token)

    async def on_partitions_revoked(self, revoked):
        self.generation+=1
        if self.scheduler:
            await self.scheduler.close(cancel=True)
        try:
            await self.batch.maybe_commit(force=True,reason="revoke")
        finally:
            self.batch.tracker.forget(revoked)
            self.paused.difference_update(revoked)
            self.scheduler=None

    async def on_partitions_assigned(self, assigned):
        self.generation+=1
        if not self.stopping:
            self.new_scheduler()

    async def metrics(self):
        if self.scheduler:
            payload=self.scheduler.metrics()
            payload["unacknowledged_records"]=self.batch.outstanding()
            payload["paused_partitions"]=len(self.paused)
            self.log(payload)
            from ..spool_lifecycle import manager
            from ..shared_media import _io
            self.log({"type":"perf","op":"media_reconciliation_queue",**await _io(manager().handoff_summary)})
        assigned=self.consumer.assignment()
        if assigned:
            try:
                ends=await self.consumer.end_offsets(assigned)
                lags={}
                for tp in assigned:
                    committed=await self.consumer.committed(tp)
                    lags[str(tp)]=None if committed is None else max(0,ends[tp]-committed)
                self.log({"type":"perf","op":"media_kafka_backlog","lane":self.name,
                          "lag_by_partition":lags,"lag_records":sum(v for v in lags.values() if v is not None)})
            except Exception as exc:
                self.log({"type":"warn","op":"media_backlog_metrics","error":type(exc).__name__})

    async def refresh_retry_keys(self):
        from ..spool_lifecycle import manager
        from ..shared_media import _io
        async with self.retry_refresh_lock:
            records=await _io(manager().retry_records)
            keys={key for record in records for key in self.describe_work(record).keys}
            self.retry_keys.clear();self.retry_keys.update(keys)
            if self.scheduler:self.scheduler.changed.set()

    async def retry_loop(self):
        from ..spool_lifecycle import manager
        from ..shared_media import _io
        from ..runtime.bot_runtime import DEFER_MEDIA_RETRIES
        class NoCommit:
            def mark_processed(self,record):pass
            async def maybe_commit(self,**kwargs):pass
        while True:
            record=await _io(manager().claim_retry)
            if record is None:
                await asyncio.sleep(1)
                continue
            token=DEFER_MEDIA_RETRIES.set(True)
            try:
                confirmed=await self.handler(record,NoCommit())
                await _io(manager().finish_retry,record,confirmed)
                await self.refresh_retry_keys()
                self.log({'type':'perf','op':'media_retry_result','confirmed':bool(confirmed),
                          'partition':record.partition,'offset':record.offset})
            finally:DEFER_MEDIA_RETRIES.reset(token)

    async def run(self):
        self.consumer.subscribe([self.topic],listener=self)
        retry_task=None
        try:
            await self.refresh_retry_keys()
            await self.consumer.start()
            retry_task=asyncio.create_task(self.retry_loop())
            while True:
                if retry_task.done():retry_task.result()
                scheduler=self.scheduler
                assigned=self.consumer.assignment()
                # Outstanding offsets include unconfirmed jobs, not just active
                # tasks. This is a bounded fence, never a poison-message skip.
                free=max(0,self.capacity-self.batch.outstanding())
                should_pause=assigned if not scheduler or free==0 or scheduler.total>=self.capacity else set()
                to_pause=should_pause-self.paused
                to_resume=(self.paused-should_pause)&assigned
                if to_pause:self.consumer.pause(*to_pause)
                if to_resume:self.consumer.resume(*to_resume)
                self.paused=(self.paused|to_pause)-to_resume
                generation=self.generation
                records=await self.consumer.getmany(timeout_ms=250,max_records=max(1,min(32,free)))
                if generation!=self.generation or scheduler is not self.scheduler:
                    # A rebalance can complete while getmany is awaiting data.
                    # Never silently discard fetched records then acknowledge a
                    # later offset: rewind to the last broker-confirmed receipt.
                    for tp in self.consumer.assignment():
                        committed=await self.consumer.committed(tp)
                        if committed is not None:self.consumer.seek(tp,committed)
                        else:await self.consumer.seek_to_beginning(tp)
                elif scheduler:
                    for tp,items in records.items():
                        if tp not in self.consumer.assignment():continue
                        for record in items:
                            self.batch.register(record)
                            await scheduler.submit(record)
                await self.batch.maybe_commit()
                if time.monotonic()-self.last_metrics>=30:
                    await self.metrics()
                    self.last_metrics=time.monotonic()
        finally:
            self.stopping=True
            if retry_task:
                retry_task.cancel()
                await asyncio.gather(retry_task,return_exceptions=True)
            if self.scheduler:
                await self.scheduler.close(cancel=True)
            try:
                await self.batch.maybe_commit(force=True,reason="shutdown")
            finally:
                await self.consumer.stop()
