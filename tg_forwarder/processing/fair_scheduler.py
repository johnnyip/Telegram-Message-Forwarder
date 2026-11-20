"""Bounded fair media scheduling. Unknown jobs fence offset commits."""
import asyncio
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
import json
import time

class ContiguousOffsets:
    def __init__(self):
        self.pending = defaultdict(deque)
        self.done = defaultdict(set)
        self.ready = {}

    def register(self, partition, offset):
        q = self.pending[partition]
        if q and offset <= q[-1]:
            raise ValueError("Offsets must be admitted in partition order")
        q.append(offset)

    def complete(self, partition, offset):
        if offset not in self.pending[partition]:
            raise ValueError("Unregistered offset completion")
        self.done[partition].add(offset)
        q = self.pending[partition]
        while q and q[0] in self.done[partition]:
            value = q.popleft()
            self.done[partition].remove(value)
            self.ready[partition] = value + 1

    def forget(self, partitions):
        for p in partitions:
            self.pending.pop(p, None)
            self.done.pop(p, None)
            self.ready.pop(p, None)

@dataclass
class Work:
    record: object
    source: object
    keys: tuple
    lane: str
    due: float
    created: float
    enqueued: float


def describe(record, *, slow_bytes=20*1024*1024, route_map=None):
    """Immutable metadata only; never stat NAS files on the event loop."""
    try:
        job = json.loads(record.value)
        info = job.get("info") or {}
        source = info.get("chat_id", "unknown")
        sender = info.get("sender_id")
        identity = ("sender", sender) if sender is not None else ("source", source)
        targets = [str(v[1]) for v in job.get("spool_targets", [])]
        if not targets and route_map:
            targets=[str(t) for name in job.get('route_names',[]) for t in route_map.get(name,{}).get('targets',[])]
        keys = tuple((target, identity) for target in sorted(set(targets))) or (("all", identity),)
        snapshots = job.get("snapshots") or [job.get("snapshot") or {}]
        size = sum(max(0, int(s.get("media_size") or 0)) for s in snapshots)
        lane = "slow" if size >= slow_bytes or "forward" in job.get("job_type", "") else "fast"
        due = float(job.get("due_at") or 0)
        return Work(record, source, keys, lane, due, max(0,due), time.monotonic())
    except (ValueError, TypeError, AttributeError, KeyError, IndexError):
        return Work(record, "malformed", (("malformed",),), "fast", 0, 0, time.monotonic())


class FairScheduler:
    """Round-robin sources, FIFO overlapping flows, reserved fast capacity.

    Records stay bounded until their handler finishes. The caller must also
    bound unresolved Kafka offsets: finished is not the same as acknowledged.
    """
    def __init__(self, handler, *, workers=3, capacity=96, slow_workers=1,
                 describe_work=describe, log=lambda _: None, name="media"):
        if not 1 <= workers <= 16 or capacity < workers:
            raise ValueError("Invalid bounded scheduler limits")
        self.handler = handler
        self.workers = workers
        self.capacity = capacity
        self.slow_workers = max(1, min(slow_workers, workers))
        self.describe_work = describe_work
        self.log = log
        self.name = name
        self.queues = defaultdict(deque)
        self.sources = deque()
        self.flows = defaultdict(deque)
        self.active_keys = set()
        self.blocked_keys = set()
        self.running = {}
        self.active_lanes = Counter()
        self.counts = Counter()
        self.total = 0
        self.changed = asyncio.Event()
        self.closed = False
        self.dispatcher = None
        self.latencies = deque(maxlen=1000)
        self.metrics_at=time.monotonic()
        self.metrics_counts=Counter()

    async def submit(self, record):
        while self.total >= self.capacity:
            self.changed.clear()
            if self.total < self.capacity:
                break
            await self.changed.wait()
        if self.closed:
            raise RuntimeError("Scheduler is closed")
        work = self.describe_work(record)
        if not self.queues[work.source]:
            self.sources.append(work.source)
        self.queues[work.source].append(work)
        for key in work.keys:
            self.flows[key].append(work)
        self.total += 1
        self.counts["received"] += 1
        self.changed.set()
        if self.dispatcher is None:
            self.dispatcher = asyncio.create_task(self._dispatch())

    def _choose(self):
        for _ in range(len(self.sources)):
            source = self.sources.popleft()
            queue = self.queues[source]
            chosen = None
            for work in queue:
                if work.due > time.time():
                    continue
                if work.lane == "slow" and self.active_lanes["slow"] >= self.slow_workers:
                    continue
                if any(key in self.active_keys or key in self.blocked_keys or self.flows[key][0] is not work for key in work.keys):
                    continue
                chosen = work
                break
            if chosen is not None:
                queue.remove(chosen)
            if queue:
                self.sources.append(source)
            else:
                del self.queues[source]
            if chosen is not None:
                return chosen
        return None

    async def _run(self, work):
        start = time.monotonic()
        outcome = "unconfirmed"
        try:
            confirmed = await self.handler(work.record)
            outcome = "completed" if confirmed else "unconfirmed"
        except asyncio.CancelledError:
            outcome = "canceled_unconfirmed"
            raise
        except Exception as exc:
            self.log({"type":"critical", "op":"scheduler_handler_failure", "lane":self.name,
                      "error":type(exc).__name__, "partition":work.record.partition,
                      "offset":work.record.offset})
        finally:
            self.counts[outcome] += 1
            self.latencies.append(time.monotonic()-start)
            self.log({"type":"perf", "op":"media_scheduler_job", "lane":self.name,
                      "size_lane":work.lane, "chat_id":work.source, "outcome":outcome,
                      "queue_wait_ms":round((start-work.enqueued)*1000),
                      "service_ms":round((time.monotonic()-start)*1000),
                      "age_seconds":round(max(0,time.time()-work.created),3) if work.created else None})
            for key in work.keys:
                self.active_keys.discard(key)
                q = self.flows[key]
                assert q.popleft() is work
                if not q:
                    del self.flows[key]
            self.active_lanes[work.lane] -= 1
            self.total -= 1
            self.changed.set()

    async def _dispatch(self):
        while not self.closed or self.total:
            self.changed.clear()
            for task in list(self.running):
                if task.done():
                    self.running.pop(task)
                    if not task.cancelled():
                        task.exception()
            while len(self.running) < self.workers:
                work = self._choose()
                if work is None:
                    break
                self.active_keys.update(work.keys)
                self.active_lanes[work.lane] += 1
                self.running[asyncio.create_task(self._run(work))] = work
            if self.closed and not self.total:
                break
            try:
                await asyncio.wait_for(self.changed.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass

    def metrics(self):
        queued = [w for q in self.queues.values() for w in q]
        values = sorted(self.latencies)
        elapsed=max(.001,time.monotonic()-self.metrics_at)
        rates={k:round((self.counts[k]-self.metrics_counts[k])*60/elapsed,3) for k in ('received','completed','unconfirmed')}
        self.metrics_counts=self.counts.copy();self.metrics_at=time.monotonic()
        return {"type":"perf", "op":"media_scheduler", "lane":self.name,
                "queued":len(queued), "inflight":sum(not t.done() for t in self.running),
                "capacity":self.capacity, "workers":self.workers,
                "active_lanes":dict(self.active_lanes),
                "oldest_queued_age_seconds":round(max(0,max((time.time()-w.created for w in queued if w.created),default=0)),3),
                "counts":dict(self.counts), "rates_per_minute":rates,
                "service_p95_seconds":round(values[min(len(values)-1,int(len(values)*.95))],3) if values else 0,
                "queued_by_source":{str(k):len(v) for k,v in self.queues.items()}}

    async def close(self, *, cancel=False):
        self.closed = True
        if cancel:
            queued = [w for q in self.queues.values() for w in q]
            for w in queued:
                for key in w.keys:
                    self.flows[key].remove(w)
                    if not self.flows[key]:
                        del self.flows[key]
            self.total -= len(queued)
            self.queues.clear()
            self.sources.clear()
            for task in self.running:
                task.cancel()
        self.changed.set()
        if self.dispatcher:
            await self.dispatcher
        if self.running:
            await asyncio.gather(*self.running, return_exceptions=True)
