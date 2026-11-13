"""Opt-in future comment photos on existing routes; no new client or media store.

One named Redis state per channel plus a bounded registry, no key scans. Pending
publication is replayable; existing shared per-target upload receipts arbitrate
uncertain Kafka acknowledgements and overlapping entrypoints.
"""
import asyncio
from contextvars import ContextVar
import json
import time
import uuid
from telethon import functions, types, utils
from ..shared_media import digest, message_identity, configured_bridge
from ..storage.edit_mapping import get_redis, REDIS_PREFIX
from ..control.source import Resolver
from .future_delivery import BLOCKED, evidence_key, seal, metadata_ready

PREFIX = REDIS_PREFIX + ':future_comments:'
REGISTRY = PREFIX + 'channels'
FLAG = 'forward_future_comment_photos'
_STAGE = ContextVar('future_comment_stage', default=None)


class OutboxProducer:
    """Only wraps the existing producer, retaining its exact prepared Kafka bytes."""
    def __init__(self, actual):
        self.actual = actual

    async def send_and_wait(self, topic, data):
        stage = _STAGE.get()
        if stage is None:
            raise RuntimeError('Future comment publication outside owned outbox')
        topic = topic + '-future-comments'
        try:
            await metadata_ready(self.actual.client, topic)
        except Exception as exc:
            from .future_delivery import block
            await block({'reason':type(exc).__name__, 'topic':topic})
            raise
        data = await stage(topic, data)
        result = await self.actual.send_and_wait(topic, data)
        stage.acked = True
        return result


def selections(routes):
    result = {}
    for route in routes:
        if not route.get(FLAG, False):
            continue
        for source in route['sources']:
            if type(source) is not int or source >= -1000000000000:
                raise ValueError('Future comments require explicit numeric channel sources')
            result.setdefault(source, []).append(route)
    if len(result) > 32:
        raise ValueError('Future comment source bound is 32')
    return result


def signature(routes):
    return digest(sorted((r['name'], sorted(r['sources'], key=str), sorted(r['targets'], key=str)) for r in routes))


async def permitted(info, route):
    """Sender-side fence: stale queued work cannot inherit changed destinations."""
    context = info.get('future_comments')
    if not context:
        return True
    source = context['source']
    if not route.get(FLAG, False) or source not in route['sources']:
        return False
    try:
        redis = await get_redis()
        if redis is None:
            raise RuntimeError('Future comment state unavailable')
        raw = await redis.get(PREFIX + str(source))
    except Exception:
        # This check is before transport. A typed definite rejection permits the
        # existing shared delivery claim to be safely released, not poisoned.
        from telegram.error import RetryAfter
        raise RetryAfter(5) from None
    state = json.loads(raw) if raw else {}
    return (state.get('active') is True and state.get('generation') == context['generation']
            and state.get('signature') == context['signature']
            and any(r['name'] == route['name'] and r['targets'] == list(route['targets'])
                    for r in state.get('routes', [])))


class FutureComments:
    def __init__(self, *, client, routes, publish_media, publish_album, resolve_info,
                 should_ignore, should_skip, log, run_api, redis_factory=get_redis,
                 bridge_factory=configured_bridge, interval=15, settle=30, page_size=50,
                 clock=time.time, replay_job=None):
        if not 10 <= page_size <= 99 or interval < 1 or settle < 1:
            raise ValueError('Future comment polling bounds')
        self.replay_job = replay_job
        self.client, self.routes = client, routes
        self.publish_media, self.publish_album = publish_media, publish_album
        self.resolve_info, self.should_ignore, self.should_skip = resolve_info, should_ignore, should_skip
        self.log, self.run_api = log, run_api
        self.redis_factory, self.bridge_factory = redis_factory, bridge_factory
        self.interval, self.settle, self.page_size, self.clock = interval, settle, page_size, clock
        self.wakeup = asyncio.Event()
        self.groups = set()
        self.lock = asyncio.Lock()
        self.disabled_complete = False
        selections(routes)  # fail configuration validation before any source reads

    def offer(self, message):
        # No RPC/download/lookup in the receive handler, including the no-route path.
        if message.chat_id in self.groups:
            self.wakeup.set()

    async def call(self, fn, *args, **kwargs):
        return await asyncio.wait_for(self.run_api(lambda: fn(*args, **kwargs), op='future_comments', extra={}), timeout=20)

    async def loop(self):
        while True:
            try:
                await asyncio.wait_for(self.tick(), timeout=110)
                if not selections(self.routes) and self.disabled_complete:
                    return  # persist disabled generations once; no idle default-off polling
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log({'type':'err', 'op':'future_comments_tick', 'error':type(exc).__name__})
            self.wakeup.clear()
            try:
                await asyncio.wait_for(self.wakeup.wait(), self.interval)
            except asyncio.TimeoutError:
                pass

    async def tick(self):
        async with self.lock:
            self.disabled_complete = False
            busy = False
            selected = selections(self.routes)
            redis = await self.redis_factory()
            if redis is None and not selected:
                return
            if redis is None:
                raise RuntimeError('Future comments require existing Redis')
            # Shared receipts are mandatory, not an optional best-effort in this lane.
            if selected and await self.bridge_factory() is None:
                raise RuntimeError('Future comments require MEDIA_SHARED_ENABLED receipts')
            selected = selections(self.routes)
            raw = await redis.get(REGISTRY)
            known = json.loads(raw) if raw else []
            if not isinstance(known, list) or len(known) > 32:
                raise RuntimeError('Invalid future comment registry')
            for source in sorted(set(known) | set(selected)):
                if len(set(known) | set(selected)) > 32:
                    raise RuntimeError('Future comment registry full; explicit maintenance required')
                token = uuid.uuid4().hex
                lease = PREFIX + str(source) + ':lock'
                if not await redis.set(lease, token, nx=True, ex=120):
                    busy = True
                    continue
                try:
                    key = PREFIX + str(source)
                    raw = await redis.get(key)
                    state = json.loads(raw) if raw else {'generation':0, 'active':False}
                    routes = selected.get(source, [])
                    if source not in known:
                        # Registry precedes state publication: a crash cannot orphan active state.
                        registered = await redis.eval("local raw=redis.call('GET',KEYS[1]); local list=raw and cjson.decode(raw) or {}; for _,v in ipairs(list) do if v==tonumber(ARGV[1]) then return cjson.encode(list) end end; if #list>=32 then return redis.error_reply('registry bound') end; table.insert(list,tonumber(ARGV[1])); local value=cjson.encode(list); redis.call('SET',KEYS[1],value); return value",1,REGISTRY,str(source))
                        known = json.loads(registered)
                    try:
                        await self.channel(redis, key, lease, token, source, routes, state)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        state['error'] = type(exc).__name__
                        await self.save(redis, key, lease, token, state)
                        self.log({'type':'err', 'op':'future_comments_source', 'source':source,
                                  'error':type(exc).__name__})
                finally:
                    await redis.eval("if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end",1,lease,token)
            self.disabled_complete = not selected and not busy

    async def save(self, redis, key, lease, token, state):
        saved = await redis.eval("if redis.call('GET',KEYS[1]) == ARGV[1] then redis.call('SET',KEYS[2],ARGV[2]); return 1 else return 0 end",
                                 2, lease, key, token, json.dumps(state, sort_keys=True))
        if not saved:
            raise RuntimeError('Future comment lease lost')

    async def linked(self, source):
        channel = await self.call(self.client.get_entity, source)
        if not getattr(channel, 'broadcast', False):
            raise ValueError('Future comments require a broadcast channel')
        full = await self.call(self.client, functions.channels.GetFullChannelRequest(channel))
        gid = getattr(full.full_chat, 'linked_chat_id', None)
        if not gid:
            raise ValueError('Channel has no linked discussion')
        group = await self.call(self.client.get_entity, types.PeerChannel(gid))
        reverse = await self.call(self.client, functions.channels.GetFullChannelRequest(group))
        if getattr(reverse.full_chat, 'linked_chat_id', None) != channel.id:
            raise ValueError('Discussion/channel link is not reciprocal')
        self.groups.add(utils.get_peer_id(group))
        return channel, group

    async def history(self, group, cursor):
        # Same ascending offset convention as Telethon iter_messages(reverse=True).
        page = await self.call(self.client, functions.messages.GetHistoryRequest(
            peer=group, offset_id=cursor+1, offset_date=None, add_offset=-(self.page_size+1),
            limit=self.page_size+1, max_id=0, min_id=cursor, hash=0))
        values = sorted((m for m in page.messages if getattr(m,'id',0)>cursor), key=lambda m:m.id)
        if len({m.id for m in values}) != len(values):
            raise ValueError('Duplicate history IDs')
        return values

    async def parent(self, msg, channel, group, resolver):
        reply = getattr(msg, 'reply_to', None)
        mid = getattr(reply, 'reply_to_top_id', None) or getattr(reply, 'reply_to_msg_id', None)
        visited = {msg.id}
        for _ in range(16):
            if not mid:
                return None
            if mid in visited:
                raise ValueError("Cyclic comment ancestry")
            visited.add(mid)
            root = await self.call(self.client.get_messages, group, ids=mid)
            if not root:
                raise ValueError('Comment ancestor unavailable')
            forward = getattr(root, 'fwd_from', None)
            post = getattr(forward, 'channel_post', None)
            sender = getattr(getattr(root,'from_id',None),'channel_id',None)
            origin = getattr(getattr(forward,'from_id',None),'channel_id',None)
            if sender == channel.id and origin == channel.id and post:
                discussion = await self.call(self.client, functions.messages.GetDiscussionMessageRequest(channel,post))
                anchors = [m for m in discussion.messages if m.chat_id == utils.get_peer_id(group)]
                verified = []
                for anchor in anchors:
                    verified.extend(await resolver.album(group,anchor))
                ids = {m.id for m in verified}
                if msg.id in ids:
                    return None  # every member of the automatic outer/root album
                if root.id not in ids:
                    raise ValueError('Forwarded author is not the verified discussion root')
                original = await self.call(self.client.get_messages, channel, ids=post)
                if not original:
                    raise ValueError('Original channel post unavailable')
                if getattr(root,'noforwards',False) or getattr(original,'noforwards',False):
                    return None
                return {'chat_id':utils.get_peer_id(channel), 'post_id':min(
                    (getattr(getattr(m,'fwd_from',None),'channel_post',post) or post for m in verified), default=post),
                    'thread':{'chat':utils.get_peer_id(group),'id':min(ids),'root_ids':sorted(ids)}}
            reply = getattr(root,'reply_to',None)
            mid = getattr(reply,'reply_to_top_id',None) or getattr(reply,'reply_to_msg_id',None)
        raise ValueError('Comment nesting exceeds lookup bound')

    async def preserve_pending(self, redis, state):
        pending = state.get('pending')
        if not pending:
            return
        evidence = state.setdefault('evidence', [])
        identity = pending.get('evidence_key')
        if identity:
            from .future_delivery import transition
            await transition(redis,identity,'canceled',expected_job=pending.get('job'))
        else:
            identity = PREFIX+'canceled:'+digest(pending)
            await redis.set(identity,json.dumps({'status':'canceled','pending':pending},sort_keys=True))
        if identity not in evidence:
            if len(evidence)>=32:
                raise RuntimeError('Future evidence bound; explicit reconciliation required')
            evidence.append(identity)

    async def channel(self, redis, key, lease, token, source, routes, state):
        if not routes:
            if state.get('active'):
                await self.preserve_pending(redis,state)
                state.update(active=False,generation=state['generation']+1,pending=None,tail=None,error='')
                await self.save(redis,key,lease,token,state)
            return
        if await redis.get(BLOCKED):
            if state.get('active') and state.get('signature') != signature(routes):
                await self.preserve_pending(redis,state)
                state.update(active=False,generation=state['generation']+1,pending=None,tail=None)
                await self.save(redis,key,lease,token,state)
            return
        channel, group = await self.linked(source)
        group_id = utils.get_peer_id(group)
        sig = signature(routes)
        if not state.get('active') or state.get('signature') != sig or state.get('group') != group_id:
            await self.preserve_pending(redis,state)
            latest = await self.call(self.client.get_messages,group,limit=1)
            cursor = max((m.id for m in latest),default=0)
            state.update(active=True, generation=state['generation']+1, signature=sig, group=group_id,
                         cursor=cursor, enabled_at=self.clock(), pending=None, tail=None, error='',
                         routes=[{'name':r['name'],'targets':list(r['targets'])} for r in routes])
            await self.save(redis,key,lease,token,state)
            return  # activation never publishes existing messages
        # Bounded terminal history: retire ONLY explicitly complete records. Shared
        # physical delivery receipts remain permanent. Uncertain/canceled records
        # are never silently pruned; a full non-complete history requires review.
        retiring = state.get('retiring')
        if retiring:
            raw = await redis.get(retiring)
            if raw and json.loads(raw).get('status') != 'complete':
                raise RuntimeError('Future evidence retirement changed; reconcile')
            if raw:
                await redis.eval("if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end",1,retiring,raw)
            state['evidence'] = [v for v in state.get('evidence',[]) if v != retiring]
            state.pop('retiring',None)
            await self.save(redis,key,lease,token,state)
        if len(state.get('evidence',[])) >= 32:
            for identity in state['evidence']:
                raw = await redis.get(identity)
                if raw and json.loads(raw).get('status') == 'complete':
                    state['retiring'] = identity
                    await self.save(redis,key,lease,token,state)
                    return  # retire exact named record next tick; crash-resumable
            raise RuntimeError('Future unresolved evidence bound; explicit maintenance required')
        if getattr(channel,'noforwards',False) or getattr(group,'noforwards',False):
            raise PermissionError('Protected parent/discussion; cursor preserved')
        bridge = await self.bridge_factory()
        resolver = Resolver(self.client,limit=201,gate=bridge)
        pending = state.get('pending')
        if pending:
            if pending.get('acked'):
                raw = await redis.get(pending['evidence_key'])
                evidence = json.loads(raw) if raw else {}
                if evidence.get('status') != 'complete':
                    return
                state.setdefault('evidence', []).append(pending['evidence_key'])
                if len(state['evidence']) > 32:
                    raise RuntimeError('Future evidence bound; explicit reconciliation required')
                state.update(pending=None)
                await self.save(redis,key,lease,token,state)
                pending = None
        if pending:
            messages = await self.call(self.client.get_messages,group,ids=pending['ids'])
            messages = sorted([m for m in messages if m],key=lambda m:m.id)
            if ([m.id for m in messages] != pending['ids'] or
                    [message_identity(m,group_id) for m in messages] != pending['identities']):
                raise ValueError('Pending comment selection changed; reconciliation required')
            await self.publish(redis,key,lease,token,state,source,routes,channel,group,resolver,messages,pending['advance'])
            if state.get('pending'):
                return
        messages = await self.history(group,state['cursor'])
        # The extra item is a boundary probe, not permission to skip excess backlog.
        boundary = messages[self.page_size] if len(messages)>self.page_size else None
        messages = messages[:self.page_size]
        index = 0
        while index < len(messages):
            first = messages[index]
            gid = getattr(first,'grouped_id',None)
            batch = [first]; index += 1
            if gid:
                while index<len(messages) and getattr(messages[index],'grouped_id',None)==gid:
                    batch.append(messages[index]);index+=1
                if index==len(messages) and boundary is not None and getattr(boundary,"grouped_id",None)==gid:
                    # Never publish the truncated tail of a page-spanning album.
                    return
                if len(batch)>10:
                    raise ValueError('Comment album exceeds Telegram bound')
                stamp = digest([(m.id,str(getattr(getattr(m,"photo",None) or getattr(m,"document",None),"id",""))) for m in batch])
                tail = state.get('tail') or {}
                if tail.get('signature') != stamp:
                    state['tail'] = {'signature':stamp,'since':self.clock()}
                    await self.save(redis,key,lease,token,state)
                    return
                if self.clock()-tail['since'] < self.settle:
                    return
            await self.publish(redis,key,lease,token,state,source,routes,channel,group,resolver,batch,batch[-1].id)
            if state.get('pending'):
                return

    async def publish(self, redis, key, lease, token, state, source, routes, channel, group, resolver, batch, advance):
        photos = []
        origin = None
        for msg in batch:
            if not getattr(msg,'photo',None) or getattr(msg,'noforwards',False) or self.should_skip(msg):
                continue
            parent = await self.parent(msg,channel,group,resolver)
            if parent is None:
                continue  # text, unrelated group messages and forwarded root copies are never jobs
            if origin is not None and origin != parent:
                raise ValueError('Album spans distinct discussion roots')
            info = await self.resolve_info(msg,chat_id_hint=msg.chat_id)
            if self.should_ignore(info):
                continue
            origin = parent
            photos.append((msg,info))
        if photos:
            ids = [m.id for m,_ in photos]
            identities = [message_identity(m,state['group']) for m,_ in photos]
            if not state.get('pending'):
                state['pending'] = {'ids':ids,'identities':identities,'advance':advance}
                await self.save(redis,key,lease,token,state)
            # Disable/target changes win after every awaited mapping, before enqueue.
            if signature(selections(self.routes).get(source,[])) != state['signature']:
                raise ValueError('Future comment route changed before publication')
            if await redis.get(lease) != token:
                raise RuntimeError('Future comment lease lost before publication')
            info = dict(photos[0][1])
            info['discussion_source'] = origin
            info['future_comments'] = {'source':source,'generation':state['generation'],'signature':state['signature'], 'expected':identities}
            usable = [m for m,_ in photos]
            async def stage(topic, data):
                if len(data)>1024*1024:
                    raise ValueError('Future comment outbox payload bound')
                job = json.loads(data)
                if job.get('info',{}).get('future_comments') != info['future_comments']:
                    raise ValueError('Future prepared context mismatch')
                await seal(job)
                data = json.dumps(job,sort_keys=True,separators=(',',':')).encode()
                identity = evidence_key(job)
                prepared_job = {'topic':topic,'data':data.decode()}
                created = await redis.set(identity,json.dumps({'status':'prepared', 'job':prepared_job},sort_keys=True),nx=True)
                if not created:
                    existing = json.loads(await redis.get(identity) or '{}')
                    if existing.get('job') != prepared_job:
                        raise ValueError('Future prepared evidence is immutable; reconcile changed snapshot')
                state['pending']['job'] = prepared_job
                state['pending']['evidence_key'] = identity
                await self.save(redis,key,lease,token,state)
                if signature(selections(self.routes).get(source,[])) != state['signature']:
                    raise ValueError('Future comment disabled while staging publication')
                return data
            prepared = state['pending'].get('job')
            if prepared is not None:
                if self.replay_job is None:
                    raise RuntimeError('Future comment outbox replay adapter unavailable')
                await self.replay_job(prepared['topic'],prepared['data'])
                stage.acked = True
            else:
                scope = _STAGE.set(stage)
                try:
                    if getattr(batch[0],'grouped_id',None):
                        await self.publish_album(usable,info,routes,'discussion')
                    else:
                        await self.publish_media(usable[0],info,routes,'discussion')
                finally:
                    _STAGE.reset(scope)
            if not state['pending'].get('job') or not getattr(stage,'acked',False):
                raise RuntimeError('Future publisher produced no complete prepared job')
            state['pending']['acked'] = True
            state.update(cursor=advance,tail=None,error='')
            await self.save(redis,key,lease,token,state)
            return
        # Newly protected/ignored pending selections retain cancellation evidence too.
        await self.preserve_pending(redis,state)
        # Only acknowledged publication (or proven irrelevant input) advances state.
        state.update(cursor=advance,pending=None,tail=None,error='')
        await self.save(redis,key,lease,token,state)
