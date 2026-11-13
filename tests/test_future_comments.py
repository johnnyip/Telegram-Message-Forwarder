"""No sockets: actual collector, requests, inherited publishers and main entrypoints."""
import ast
import asyncio
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
import tempfile
from unittest.mock import AsyncMock, patch
from telethon import functions, types
from tg_forwarder.processing.future_comments import FutureComments, PREFIX, REGISTRY, FLAG, permitted, signature
from tg_forwarder.domain.routes import load_routes, find_matching_routes
from tg_forwarder.shared_media import SharedMedia, media_identity, digest
from test_shared_media import Redis as BaseRedis

SOURCE=-1000000000011
GROUP=-1000000000022


class Redis(BaseRedis):
    async def eval(self, script, count, *args):
        if 'future_evidence_cas_v1' in script:
            key,old,new=args
            hook=getattr(self,'before_evidence_cas',None)
            if hook is not None:
                await hook(key,old,new)
            # Model one Redis command atomically: no await between compare/write.
            if self.data.get(key)!=old:return 0
            self.data[key]=new
            return 1
        if count == 2:
            lease,key,token,value=args
            if await self.get(lease)!=token:return 0
            await self.set(key,value);return 1
        if 'table.insert' in script:
            key,source=args;values=json.loads(await self.get(key) or '[]')
            if int(source) not in values:values.append(int(source))
            await self.set(key,json.dumps(values));return json.dumps(values)
        return await super().eval(script,count,*args)


def message(mid, *, photo=True, album=None, reply=1, top=None, protected=False):
    return NS(id=mid,chat_id=GROUP,photo=NS(id=70000+mid) if photo else None,
        document=None,media=True,grouped_id=album,noforwards=protected,date=datetime.now(timezone.utc),
        message='caption',text='caption',file=NS(size=4),from_id=types.PeerUser(9),fwd_from=None,
        reply_to=NS(reply_to_msg_id=reply,reply_to_top_id=top) if reply else None)


class Client:
    def __init__(self):
        self.channel=NS(id=11,peer=SOURCE,broadcast=True,noforwards=False)
        self.group=NS(id=22,peer=GROUP,broadcast=False,noforwards=False)
        self.root=message(1,reply=None)
        self.root.from_id=types.PeerChannel(11)
        self.root.fwd_from=NS(from_id=types.PeerChannel(11),channel_post=50)
        self.original=NS(id=50,noforwards=False)
        self.messages={1:self.root};self.discussion_roots={50:[self.root]};self.calls=[];self.fail_history=False
    async def get_entity(self,peer):
        return self.channel if peer==SOURCE else self.group
    async def get_messages(self,peer,ids=None,limit=None):
        if limit:return [self.messages[max(self.messages)]]
        if peer is self.channel:return self.original
        if isinstance(ids,list):return [self.messages[i] for i in ids if i in self.messages]
        return self.messages.get(ids)
    async def iter_messages(self,peer,limit,offset_id=None,min_id=None,reverse=False):
        values=sorted(self.messages.values(),key=lambda m:m.id,reverse=not reverse)
        for m in values:
            if offset_id is not None and m.id>=offset_id:continue
            if min_id is not None and m.id<=min_id:continue
            yield m

    async def __call__(self,req):
        self.calls.append(req)
        if isinstance(req,functions.channels.GetFullChannelRequest):
            return NS(full_chat=NS(linked_chat_id=22 if req.channel is self.channel else 11))
        if isinstance(req,functions.messages.GetDiscussionMessageRequest):
            return NS(messages=self.discussion_roots.get(req.msg_id,[self.root]))
        if isinstance(req,functions.messages.GetHistoryRequest):
            if self.fail_history:raise PermissionError('not joined/read forbidden')
            values=sorted((m for i,m in self.messages.items() if i>req.min_id),key=lambda m:m.id)
            return NS(messages=list(reversed(values[:req.limit])))
        raise AssertionError(type(req).__name__)


def extract(name,env):
    node=next(n for n in ast.parse(Path('main.py').read_text()).body if isinstance(n,(ast.AsyncFunctionDef,ast.FunctionDef)) and n.name==name)
    node.returns=None
    for a in node.args.args:a.annotation=None
    exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),env)
    return env[name]


class FutureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.peer_patch=patch('tg_forwarder.processing.future_comments.utils.get_peer_id',lambda e:e.peer)
        self.peer_patch.start();self.addCleanup(self.peer_patch.stop)
        self.redis=Redis();self.client=Client();self.now=1000.
        self.routes=[{'name':'existing','sources':{SOURCE},'targets':[2],FLAG:True}]
        self.media=AsyncMock();self.album=AsyncMock();self.logs=[]
        self.bridge=NS(wait=AsyncMock(),delay=lambda s:s)
        async def run(fn,**kw):return await fn()
        async def info(m,**kw):return {'chat_id':m.chat_id,'msg_id':m.id,'sender_id':9,'grouped_id':m.grouped_id,'chat_title':'discussion','sender_type':'user','sender_username':'user','sender_display':'User','chat_noforwards':False,'msg_noforwards':False}
        self.kw=dict(client=self.client,routes=self.routes,publish_media=self.media,publish_album=self.album,
            resolve_info=info,should_ignore=lambda i:False,should_skip=lambda m:None,log=self.logs.append,
            run_api=run,redis_factory=AsyncMock(return_value=self.redis),bridge_factory=AsyncMock(return_value=self.bridge),
            settle=3,interval=1,page_size=10,clock=lambda:self.now)
        self.worker=FutureComments(**self.kw)
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.metadata=patch('tg_forwarder.processing.future_comments.metadata_ready',AsyncMock())
        self.metadata.start();self.addCleanup(self.metadata.stop)
        self.default_media=self.fake_media
        self.default_album=self.fake_album
        self.media.side_effect=self.default_media;self.album.side_effect=self.default_album

    def payload(self, messages, info, routes):
        from tg_forwarder.shared_media import message_identity
        values=[]
        for msg in messages:
            path=Path(self.tmp.name)/str(msg.id);path.write_bytes(b'phot')
            values.append(dict(path=str(path),media_type='photo',msg_id=msg.id,
                shared_identity=message_identity(msg,GROUP),caption_text='caption'))
        return dict(job_type='media_album_file' if len(messages)>1 or messages[0].grouped_id else 'media_file',
                    info=info,route_names=[r['name'] for r in routes],source_kind='discussion',
                    **({'snapshots':values} if len(messages)>1 or messages[0].grouped_id else {'snapshot':values[0]}))

    async def fake_publish(self, messages, info, routes, kind):
        from tg_forwarder.processing.future_comments import OutboxProducer
        from tg_forwarder.processing.future_delivery import evidence_key
        async def send(topic,data):
            job=json.loads(data);key=evidence_key(job)
            raw=json.loads(await self.redis.get(key));raw['status']='complete'
            await self.redis.set(key,json.dumps(raw))
        await OutboxProducer(NS(client=object(),send_and_wait=send)).send_and_wait('existing-media',json.dumps(self.payload(messages,info,routes)).encode())

    async def fake_media(self,msg,info,routes,kind):
        await self.fake_publish([msg],info,routes,kind)

    async def fake_album(self,msgs,info,routes,kind):
        await self.fake_publish(msgs,info,routes,kind)

    async def state(self):return json.loads(await self.redis.get(PREFIX+str(SOURCE)))
    async def enable(self):await self.worker.tick()
    def add(self,mid,**kw):
        m=message(mid,**kw);self.client.messages[mid]=m;return m

    async def test_default_off_strict_flag_no_source_calls(self):
        for value in ('true',1,None):
            with self.assertRaises(RuntimeError):load_routes(json.dumps([{'sources':[SOURCE],'targets':[2],FLAG:value}]))
        self.routes[0][FLAG]=False;await self.worker.tick()
        self.assertFalse(self.client.calls);self.media.assert_not_awaited()
        self.assertFalse(load_routes(json.dumps([{'sources':[SOURCE],'targets':[2]}]))[0][FLAG])
        with self.assertRaises(RuntimeError):load_routes(json.dumps([{'sources':['@channel'],'targets':[2],FLAG:True}]))

    async def test_baseline_old_post_new_comment_and_future_post(self):
        self.add(10);await self.enable();self.media.assert_not_awaited()
        self.add(11);await self.worker.tick();self.assertEqual(self.media.await_count,1)
        args=self.media.await_args.args;self.assertEqual(args[0].id,11);self.assertEqual(args[2],self.routes)
        self.assertEqual(args[1]['discussion_source']['post_id'],50)
        self.add(12);await self.worker.tick();self.assertEqual(self.media.await_count,2)
        await self.worker.tick();self.assertEqual(self.media.await_count,2)
        self.assertEqual((await self.state())['cursor'],12)

    async def test_nested_replies_deleted_unknown_unrelated_and_spoof_root(self):
        await self.enable()
        self.add(2,photo=False);self.add(3,reply=2)
        self.add(4,photo=False,reply=None);self.add(5,reply=4)
        spoof=self.add(6,photo=False,reply=None);spoof.from_id=types.PeerUser(9);spoof.fwd_from=self.client.root.fwd_from
        self.add(7,reply=6);await self.worker.tick()
        self.assertEqual([c.args[0].id for c in self.media.await_args_list],[3])
        await self.worker.tick();self.add(8,reply=999);await self.worker.tick();self.assertEqual((await self.state())['cursor'],7)
        self.assertTrue((await self.state())['error'])

    async def test_actual_sender_root_must_match_discussion_mapping(self):
        await self.enable();fake=self.add(2,photo=False,reply=None)
        fake.from_id=self.client.root.from_id;fake.fwd_from=self.client.root.fwd_from
        self.add(3,reply=2);await self.worker.tick();self.media.assert_not_awaited()
        self.assertEqual((await self.state())['cursor'],2)

    async def test_root_outer_album_text_video_are_not_forwarded(self):
        await self.enable();self.add(2,photo=False);self.add(3,reply=None)
        self.add(4,protected=True);await self.worker.tick();self.media.assert_not_awaited()
        self.assertEqual((await self.state())['cursor'],4)

    async def test_mixed_album_settles_late_out_of_order_and_restart(self):
        await self.enable();self.add(3,album=77);self.add(2,album=77);self.add(4,album=77,photo=False)
        await self.worker.tick();self.album.assert_not_awaited()
        self.now+=2;self.add(5,album=77);await self.worker.tick();self.now+=2
        self.worker=FutureComments(**self.kw);await self.worker.tick();self.album.assert_not_awaited()
        self.now+=2;await self.worker.tick()
        self.assertEqual([m.id for m in self.album.await_args.args[0]],[2,3,5])
        self.assertEqual(self.album.await_args.args[2],self.routes)
        self.assertEqual((await self.state())['cursor'],5)

    async def test_paging_boundary_does_not_split_album_or_skip_overflow(self):
        await self.enable()
        for mid in range(2,10):self.add(mid,photo=False)
        for mid in range(10,14):self.add(mid,album=88)
        await self.worker.tick();self.assertEqual((await self.state())['cursor'],9)
        await self.worker.tick();self.album.assert_not_awaited();self.now+=4
        await self.worker.tick();self.assertEqual([m.id for m in self.album.await_args.args[0]],[10,11,12,13])
        self.assertEqual((await self.state())['cursor'],13)

    async def test_ack_loss_pending_restart_replays_same_selection(self):
        await self.enable();self.add(2);self.media.side_effect=ConnectionError('Kafka ack lost')
        await self.worker.tick();state=await self.state();self.assertEqual(state['cursor'],1)
        self.assertEqual(state['pending']['ids'],[2])
        self.media.side_effect=self.default_media;self.worker=FutureComments(**self.kw);await self.worker.tick()
        self.assertEqual(self.media.await_count,2);self.assertEqual((await self.state())['cursor'],2)
        self.assertEqual(self.media.await_args_list[0].args[1],self.media.await_args_list[1].args[1])

    async def test_pending_edit_or_deleted_selection_fail_closed(self):
        await self.enable();self.add(2);self.media.side_effect=ConnectionError();await self.worker.tick()
        self.client.messages[2].photo.id+=1;self.media.side_effect=self.default_media
        await self.worker.tick();self.assertEqual(self.media.await_count,1)
        self.assertEqual((await self.state())['cursor'],1)

    async def test_disable_during_lookup_and_reenable_no_disabled_backfill(self):
        await self.enable();self.add(2)
        original=self.client.get_messages
        async def disable(*a,**kw):
            result=await original(*a,**kw)
            if kw.get('ids')==1:self.routes[0][FLAG]=False
            return result
        self.client.get_messages=disable;await self.worker.tick();self.media.assert_not_awaited()
        await self.worker.tick();self.assertFalse((await self.state())['active'])
        self.client.get_messages=original;self.add(3);self.routes[0][FLAG]=True
        await self.worker.tick();self.assertEqual((await self.state())['cursor'],3)
        self.add(4);await self.worker.tick();self.assertEqual(self.media.await_args.args[0].id,4)

    async def test_target_change_rebaselines_and_sender_fence(self):
        await self.enable();self.add(2);await self.worker.tick();info=self.media.await_args.args[1]
        with patch('tg_forwarder.processing.future_comments.get_redis',AsyncMock(return_value=self.redis)):
            self.assertTrue(await permitted(info,self.routes[0]))
            self.routes[0]['targets']=[3]
            self.assertFalse(await permitted(info,self.routes[0]))
            self.add(3);await self.worker.tick();self.assertEqual((await self.state())['cursor'],3)
            self.assertFalse(await permitted(info,self.routes[0]))

    async def test_protection_access_failure_and_receipt_requirement_preserve_cursor(self):
        await self.enable();self.add(2);self.client.group.noforwards=True
        await self.worker.tick();self.assertEqual((await self.state())['cursor'],1)
        self.client.group.noforwards=False;self.client.fail_history=True
        await self.worker.tick();self.assertEqual((await self.state())['cursor'],1)
        self.media.assert_not_awaited();self.assertTrue(self.logs)
        self.worker.bridge_factory=AsyncMock(return_value=None)
        with self.assertRaises(RuntimeError):await self.worker.tick()

    async def test_parent_protection_ignore_and_nonreciprocal_link(self):
        await self.enable();self.add(2);self.client.original.noforwards=True
        await self.worker.tick();self.media.assert_not_awaited()
        self.client.original.noforwards=False;self.worker.should_ignore=lambda i:True
        self.add(3);await self.worker.tick();self.media.assert_not_awaited()

    async def test_actual_receive_no_route_branch_wakes_without_rpc(self):
        await self.enable();m=self.add(2)
        env=dict(_FUTURE_COMMENTS=self.worker,StepTimer=lambda:NS(ms=lambda:0),now_ts=lambda:1000,
            dedup_seen=lambda k:False,lag_record=lambda *a:None,maybe_debug_log=lambda *a:None,log=lambda x:None,
            tstamp=lambda:'now',BACKGROUND_TASKS=set(),PENDING_ALBUMS={},DOWNLOAD_CONCURRENCY=1,
            download_semaphore=NS(_value=1),lag_summary=lambda c:None,ROUTES=self.routes,
            find_matching_routes=find_matching_routes)
        handle=extract('handle_incoming_message',env)
        self.client.get_messages=AsyncMock(side_effect=AssertionError('receive RPC'))
        await asyncio.wait_for(handle(m,'new'),.1)
        self.assertTrue(self.worker.wakeup.is_set());self.media.assert_not_awaited()

    async def test_real_album_flush_preserves_existing_default_route_behavior(self):
        m=self.add(2,album=88)
        publish=AsyncMock();env=dict(PENDING_ALBUMS={('new',GROUP,88):{'messages':{2:m}}},
            ROUTES=[{'name':'direct-group','sources':{GROUP},'targets':[3]}],find_matching_routes=find_matching_routes,
            resolve_sender_info_from_message=self.kw['resolve_info'],should_ignore=lambda *a:False,IGNORE_USERS=set(),IGNORE_IDS=set(),
            log=lambda x:None,tstamp=lambda:'now',APP_MODE='listen',should_direct_forward_large_album=lambda *a:False,
            publish_album_job=publish,dedup_mark=lambda k:None,dedup_key_for_album=lambda *a:'key',schedule_discussion_snapshot=lambda *a:None)
        flush=extract('flush_album',env)
        await flush('new',GROUP,88);publish.assert_awaited_once()
        self.assertEqual(publish.await_args.args[2],env['ROUTES'])

    async def test_shared_physical_receipts_across_entrypoints_and_targets(self):
        # The production publisher/consumer already uses legacy_send. Check its
        # operation identity is independent of entrypoint and includes destination.
        import tempfile
        from tg_forwarder.shared_media import legacy_send
        with tempfile.TemporaryDirectory() as tmp:
            bridge=SharedMedia(self.redis,Path(tmp),account_rate_key='offline',min_free_bytes=0)
            send=AsyncMock(return_value=NS(sent_message_id=8))
            snap={'shared_identity':media_identity(GROUP,2,70002)};info={'sender_id':9}
            with patch('tg_forwarder.shared_media.configured_bridge',AsyncMock(return_value=bridge)),patch.dict('os.environ',{'MEDIA_DELIVERY_IDENTITY':'bot:123'}):
                await legacy_send(send,[snap],2,'caption',info,'new')
                await legacy_send(send,[snap],2,'caption',info,'discussion')
                self.assertEqual(send.await_count,1)
                await legacy_send(send,[snap],3,'caption',info,'discussion')
                self.assertEqual(send.await_count,2)

    async def test_disable_all_is_persisted_without_source_or_shared_access(self):
        await self.enable();self.routes[0][FLAG]=False;self.client.calls.clear()
        self.worker.bridge_factory=AsyncMock(side_effect=AssertionError('disabled shared'))
        await self.worker.tick();self.assertFalse((await self.state())['active'])
        self.assertFalse(self.client.calls)

    async def test_lease_loss_never_advances_or_publishes(self):
        await self.enable();self.add(2)
        original=self.worker.resolve_info
        async def lose(*a,**kw):
            self.redis.data.pop(PREFIX+str(SOURCE)+':lock',None)
            return await original(*a,**kw)
        self.worker.resolve_info=lose
        with self.assertRaises(RuntimeError):await self.worker.tick()
        self.media.assert_not_awaited();self.assertEqual((await self.state())['cursor'],1)


    async def test_new_channel_post_and_all_automatic_root_album_members_excluded(self):
        await self.enable()
        root2=self.add(20,album=90,reply=None);root2.from_id=types.PeerChannel(11)
        root2.fwd_from=NS(from_id=types.PeerChannel(11),channel_post=60)
        root3=self.add(21,album=90,reply=20);root3.from_id=types.PeerChannel(11)
        root3.fwd_from=NS(from_id=types.PeerChannel(11),channel_post=61)
        self.client.discussion_roots.update({60:[root2,root3],61:[root2,root3]})
        self.add(22,reply=21)
        await self.worker.tick();self.now+=4;await self.worker.tick()
        self.album.assert_not_awaited()
        self.assertEqual([c.args[0].id for c in self.media.await_args_list],[22])
        self.assertEqual(self.media.await_args.args[1]['discussion_source']['post_id'],60)

    async def test_staged_kafka_bytes_survive_ack_loss_without_second_snapshot(self):
        from tg_forwarder.processing.future_comments import OutboxProducer
        await self.enable();self.add(2)
        actual=NS(client=object(),send_and_wait=AsyncMock(side_effect=ConnectionError('ack lost')))
        async def publish(msg,info,routes,kind):
            await OutboxProducer(actual).send_and_wait('existing-media',json.dumps(self.payload([msg],info,routes)).encode())
        self.worker.publish_media=AsyncMock(side_effect=publish)
        await self.worker.tick();state=await self.state()
        self.assertIn('job',state['pending']);self.assertEqual(state['cursor'],1)
        replay=AsyncMock();self.kw['replay_job']=replay
        self.worker=FutureComments(**self.kw)
        await self.worker.tick();self.assertEqual((await self.state())['cursor'],2)
        replay.assert_awaited_once_with('existing-media-future-comments',state['pending']['job']['data'])
        self.media.assert_not_awaited()  # no second snapshot/download during replay

    async def test_sender_late_disable_rejects_before_real_transport(self):
        from telegram.error import BadRequest
        send=AsyncMock()
        env=dict(future_comment_permitted=AsyncMock(return_value=False),APP_MODE='send',bot=object(),
            send_file_via_bot=send,bot_send_semaphore=None,log=lambda x:None,BOT_UPLOAD_MAX_BYTES=100)
        fn=extract('send_file_to_target',env)
        with self.assertRaises(BadRequest):
            await fn(self.routes[0],2,Path('/unused'),'caption',{'future_comments':{} or {'generation':1}},'discussion')
        send.assert_not_awaited()
        await fn(self.routes[0],2,Path('/unused'),'caption',{},'new');send.assert_awaited_once()

    async def test_state_failure_is_definite_presend_rejection_not_ambiguous_claim(self):
        from telegram.error import RetryAfter
        await self.enable();self.add(2);await self.worker.tick();info=self.media.await_args.args[1]
        with patch('tg_forwarder.processing.future_comments.get_redis',AsyncMock(side_effect=ConnectionError())):
            with self.assertRaises(RetryAfter):await permitted(info,self.routes[0])

    async def test_nonreciprocal_discussion_link_never_activates(self):
        original=self.client.__class__.__call__
        async def wrong(client,req):
            if isinstance(req,functions.channels.GetFullChannelRequest) and req.channel is client.group:
                return NS(full_chat=NS(linked_chat_id=999))
            return await original(client,req)
        with patch.object(Client,'__call__',wrong):await self.worker.tick()
        self.assertFalse((await self.state())['active']);self.media.assert_not_awaited()

    async def test_future_fingerprint_replay_stable_and_distinct_generation(self):
        fn=extract('job_fingerprint',{'json':json})
        job={'info':{'future_comments':{'source':SOURCE,'generation':1,'signature':'sig'}},
             'route_names':['existing'],'snapshot':{'shared_identity':[str(GROUP),2,'70002']}}
        first=fn(job);self.assertEqual(first,fn(copy.deepcopy(job)))
        job['info']['future_comments']['generation']=2;self.assertNotEqual(first,fn(job))

    async def test_actual_kafka_album_adapter_carries_inherited_routes_not_group_rematch(self):
        await self.enable()
        publish=AsyncMock();producer=object()
        env=dict(producer=producer,_enrich_topic_hints=AsyncMock(side_effect=lambda i,r:i),
            publish_album_job_mod=publish,MEDIA_DELAY_SECONDS=0,KAFKA_MEDIA_TOPIC='media',
            future_comment_producer=lambda p,i:p,json_bytes=lambda x:json.dumps(x).encode(),log=lambda x:None,
            should_direct_forward_large_album=lambda *a:False,SPOOL_DIR=Path('/unused'),
            download_semaphore=None,run_api=None,media_type=lambda m:'photo',get_media_size=lambda m:4,
            FORWARD_POLICY='auto')
        fn=extract('publish_album_job',env)
        await fn([self.add(2,album=88)],{},self.routes,'discussion')
        self.assertEqual(publish.await_args.args[2],self.routes)
        self.assertFalse(find_matching_routes(GROUP,self.routes))


    async def test_pending_source_rpc_cannot_block_receive_handler(self):
        await self.enable();m=self.add(2);started=asyncio.Event();release=asyncio.Event()
        original=self.worker.history
        async def hold(*a):
            started.set();await release.wait();return await original(*a)
        self.worker.history=hold
        polling=asyncio.create_task(self.worker.tick());await started.wait()
        env=dict(_FUTURE_COMMENTS=self.worker,StepTimer=lambda:NS(ms=lambda:0),now_ts=lambda:1000,
            dedup_seen=lambda k:False,lag_record=lambda *a:None,maybe_debug_log=lambda *a:None,log=lambda x:None,
            tstamp=lambda:'now',BACKGROUND_TASKS=set(),PENDING_ALBUMS={},DOWNLOAD_CONCURRENCY=1,
            download_semaphore=NS(_value=1),lag_summary=lambda c:None,ROUTES=self.routes,
            find_matching_routes=find_matching_routes)
        try:
            await asyncio.wait_for(extract('handle_incoming_message',env)(m,'new'),.1)
            self.assertFalse(polling.done())
        finally:
            release.set();await polling

    async def test_protected_discussion_root_and_nested_top_id(self):
        await self.enable();self.client.root.noforwards=True;self.add(2)
        await self.worker.tick();self.media.assert_not_awaited()
        self.client.root.noforwards=False;self.add(3,reply=999,top=1)
        await self.worker.tick();self.assertEqual(self.media.await_args.args[0].id,3)


    async def test_disable_waits_for_prior_owner_lease_before_finishing(self):
        await self.enable();self.routes[0][FLAG]=False
        lease=PREFIX+str(SOURCE)+':lock';await self.redis.set(lease,'prior-owner',ex=120)
        await self.worker.tick();self.assertFalse(self.worker.disabled_complete)
        self.assertTrue((await self.state())['active'])
        self.redis.data.pop(lease);await self.worker.tick()
        self.assertTrue(self.worker.disabled_complete);self.assertFalse((await self.state())['active'])
