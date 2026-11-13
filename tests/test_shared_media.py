"""Shared bridge integration: fake Redis, real legacy snapshot/job functions, no sockets."""
import ast
import asyncio
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch
from telethon import errors
from tg_forwarder import shared_media as shared
from tg_forwarder.processing import snapshot, job_processing
from tg_forwarder.processing.discussion import DiscussionSnapshots
from tg_forwarder.domain.routes import load_routes


class Redis:
    def __init__(self):
        self.data, self.expiry = {}, {}
    async def get(self, key):
        if self.expiry.get(key, float('inf')) <= time.time():
            self.data.pop(key, None)
        return self.data.get(key)
    async def set(self, key, value, nx=False, px=None, ex=None):
        if nx and await self.get(key) is not None:
            return False
        self.data[key] = value
        if px or ex:
            self.expiry[key] = time.time() + (px / 1000 if px else ex)
        return True
    async def eval(self, script, count, key, until, ttl=None):
        if "DEL" in script:
            if await self.get(key) == until:
                self.data.pop(key,None)
                return 1
            return 0
        if float(await self.get(key) or 0) < float(until):
            await self.set(key, until, px=int(ttl))
        return 1


class SharedTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.net = patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden'))
        self.net.start(); self.addCleanup(self.net.stop)
        self.redis = Redis()
        self.bridge = shared.SharedMedia(self.redis, self.root/'archive', account_rate_key='account', min_free_bytes=0, rate_interval=.001)
        self.info = {'chat_id': -1001, 'msg_id': 17, 'sender_id': 9}
        self.msg = NS(id=17, chat_id=-1001, photo=NS(id=345), document=None, media=True,
                      file=NS(size=4, mime_type='image/jpeg'), text='caption', message='caption')
        async def download(path):
            Path(path).write_bytes(b'data'); return path
        self.msg.download_media = AsyncMock(side_effect=download)
        self.factory = patch.object(shared, 'configured_bridge', AsyncMock(return_value=self.bridge))
        self.factory.start(); self.addCleanup(self.factory.stop)
        self.snap_factory = patch.object(snapshot, 'configured_bridge', AsyncMock(return_value=self.bridge))
        self.snap_factory.start(); self.addCleanup(self.snap_factory.stop)
        self.env = patch.dict(os.environ, {'MEDIA_DELIVERY_IDENTITY':'bot:123:legacy-mirror-v1'})
        self.env.start(); self.addCleanup(self.env.stop)

    async def take(self):
        async def run(fn, **kw): return await fn()
        return await snapshot.snapshot_media_message(self.msg, self.info, 'new', self.root/'_spool', asyncio.Semaphore(1), run, lambda _:None)

    def ctx(self):
        return NS(should_ignore=lambda *a:False, ignore_users=set(), ignore_ids=set(),
                  build_header_from_info=lambda *a,**k:'header', route_map={'r':{'name':'r','targets':[1,2]}},
                  is_stale_file=lambda p:False, media_target_parallel=False, delete_after_send=True,
                  log=lambda x:None, send_file_to_target=AsyncMock(return_value=NS(sent_message_id=77)))

    async def test_snapshot_reuses_archive_and_pipeline_cleanup_never_deletes_archive(self):
        first=await self.take(); archive=await self.bridge.cached(first['shared_identity'])
        self.assertEqual(archive.read_bytes(), b'data')
        Path(first['path']).unlink()
        second=await self.take(); self.msg.download_media.assert_awaited_once()
        self.assertNotEqual(Path(second['path']), archive)
        ctx=self.ctx()
        job={'info':self.info, 'snapshot':second, 'source_kind':'new', 'route_names':['r']}
        with patch.object(job_processing, 'store_message_mapping', AsyncMock()):
            cleanup=await job_processing.process_media_file_job(job,ctx)
            self.assertEqual(ctx.send_file_to_target.await_count,2)
            # Replay restores sender result mapping instead of sending either target again.
            await job_processing.process_media_file_job(job,ctx)
            self.assertEqual(ctx.send_file_to_target.await_count,2)
        for path in cleanup: Path(path).unlink()
        self.assertEqual(archive.read_bytes(),b'data')
        values=[json.loads(v) for k,v in self.redis.data.items() if ':source:' in k]
        self.assertFalse(Path(values[0]['path']).is_absolute())

    async def test_target_topic_bot_revision_caption_are_distinct(self):
        identity=shared.message_identity(self.msg)
        op=lambda **k:self.bridge.operation([identity],k.get('dst',1),k.get('topic',None),k.get('bot','bot:1'),caption=k.get('caption','c'))
        keys={op(),op(dst=2),op(topic=7),op(bot='bot:2'),op(caption='edited')}
        self.assertEqual(len(keys),5)
        self.assertNotEqual(shared.media_identity(-1001,17,346),identity)
        send=AsyncMock(return_value=True)
        for key in keys:await self.bridge.deliver(key,send)
        self.assertEqual(send.await_count,5)
        await self.bridge.deliver(op(),send);self.assertEqual(send.await_count,5)

    async def test_uncertain_failed_cancelled_and_redis_down_do_not_resend(self):
        for result in (False, OSError('uncertain'), asyncio.CancelledError()):
            key=shared.digest(str(type(result)))
            send=AsyncMock(side_effect=result) if isinstance(result,BaseException) else AsyncMock(return_value=False)
            with self.assertRaises((shared.AmbiguousDelivery,OSError,asyncio.CancelledError)):
                await self.bridge.deliver(key,send)
            with self.assertRaises(shared.AmbiguousDelivery):await self.bridge.deliver(key,send)
            self.assertEqual(send.await_count,1)
        send=AsyncMock()
        with patch.object(self.redis,'get',AsyncMock(side_effect=ConnectionError('down'))):
            with self.assertRaises(ConnectionError):await self.bridge.deliver('unavailable',send)
        send.assert_not_awaited()

    async def test_real_album_pipeline_replays_mapping_and_preserves_edited_caption(self):
        first=await self.take()
        self.msg.id=18;self.msg.photo=NS(id=346)
        second=await self.take()
        ctx=self.ctx();ctx.route_map['r']['targets']=[1]
        ctx.send_album_to_target=AsyncMock(return_value=NS(sent_message_id=80,sent_message_ids=[80,81]))
        job={'info':dict(self.info,grouped_id=100),'snapshots':[first,second],'source_kind':'new','route_names':['r']}
        with patch.object(job_processing,'store_album_mapping',AsyncMock()) as mapping:
            await job_processing.process_media_album_file_job(job,ctx)
            await job_processing.process_media_album_file_job(job,ctx)
            self.assertEqual(ctx.send_album_to_target.await_count,1)
            self.assertEqual(mapping.await_count,2)
            self.assertEqual(mapping.await_args.args[3]['message_ids'],[80,81])
            job['source_kind']='edited'
            first['caption_text']='changed caption'
            await job_processing.process_media_album_file_job(job,ctx)
            self.assertEqual(ctx.send_album_to_target.await_count,2)

    async def test_pipeline_failure_claim_blocks_second_attempt(self):
        snap=await self.take();ctx=self.ctx();ctx.route_map['r']['targets']=[1]
        ctx.send_file_to_target=AsyncMock(return_value=False)
        job={'info':self.info,'snapshot':snap,'source_kind':'new','route_names':['r']}
        with self.assertRaises(shared.AmbiguousDelivery):await job_processing.process_media_file_job(job,ctx)
        with self.assertRaises(shared.AmbiguousDelivery):await job_processing.process_media_file_job(job,ctx)
        ctx.send_file_to_target.assert_awaited_once()

    async def test_legacy_flag_off_no_redis_or_revision_required(self):
        with patch.object(shared,'configured_bridge',AsyncMock(return_value=None)), patch.object(snapshot,'configured_bridge',AsyncMock(return_value=None)):
            self.msg.photo=True  # no revision: unchanged old behavior when off
            snap=await self.take(); self.assertNotIn('shared_identity',snap)
            self.assertTrue(await shared.legacy_send(AsyncMock(return_value=True),[snap],1,'c',self.info,'new'))

    async def test_archive_size_traversal_symlink_and_no_clobber(self):
        source=self.root/'source';source.write_bytes(b'data')
        ident=shared.media_identity(1,2,3)
        archive=await self.bridge.publish(ident,source)
        await self.bridge.publish(ident,source)
        self.assertEqual(archive.read_bytes(),b'data')
        for raw in ('../source','/tmp/file'):
            await self.redis.set(self.bridge.prefix+'source:'+shared.digest(ident),json.dumps({'path':raw,'size':4}))
            with self.assertRaises(shared.SharedUnavailable):await self.bridge.cached(ident)
        symlink=self.root/'symlink';symlink.symlink_to(source)
        with self.assertRaises(shared.SharedUnavailable):await self.bridge.publish(ident,symlink)
        self.bridge.max_bytes=3
        with self.assertRaises(shared.SharedUnavailable):await self.bridge.publish(ident,source)
        self.assertEqual(archive.read_bytes(),b'data')

    async def test_account_gate_delay_inherited_across_instances_and_failure_closed(self):
        other=shared.SharedMedia(self.redis,self.root/'other',account_rate_key='account',min_free_bytes=0,rate_interval=.001)
        await self.bridge.record_delay(0)
        with patch.object(shared.asyncio,'sleep',AsyncMock(side_effect=lambda n:self.redis.data.pop(self.bridge.rate_key+':delay',None))) as sleep:
            await other.wait()
        sleep.assert_awaited_once()
        with patch.object(self.redis,'get',AsyncMock(side_effect=ConnectionError('offline'))):
            with self.assertRaises(ConnectionError):await other.wait()

    async def test_definite_telegram_rejection_releases_claim_unknown_retains(self):
        from telegram.error import RetryAfter, BadRequest, Forbidden
        for index,error in enumerate([RetryAfter(2),BadRequest('bad'),Forbidden('denied'),errors.FloodWaitError(request=None,capture=2),errors.ChatForwardsRestrictedError(request=None)]):
            op='definite:'+str(index)
            send=AsyncMock(side_effect=error)
            with self.assertRaises(type(error)):await self.bridge.deliver(op,send)
            self.assertNotIn(self.bridge.prefix+'claim:'+op,self.redis.data)
            await self.bridge.deliver(op,AsyncMock(return_value=True))
        op='unknown'
        with self.assertRaises(TimeoutError):await self.bridge.deliver(op,AsyncMock(side_effect=TimeoutError()))
        with self.assertRaises(shared.AmbiguousDelivery):await self.bridge.deliver(op,AsyncMock(return_value=True))

    async def test_publish_thread_cancel_waits_cleanup_and_finishes_index(self):
        source=self.root/'large';source.write_bytes(b'data')
        identity=shared.media_identity(1,2,3)
        started,release=threading.Event(),threading.Event()
        original=self.bridge._publish_file
        def slow(*args):
            started.set();release.wait(3)
            return original(*args)
        with patch.object(self.bridge,'_publish_file',side_effect=slow):
            task=asyncio.create_task(self.bridge.publish(identity,source))
            self.assertTrue(await asyncio.to_thread(started.wait,1))
            # Event loop remains responsive while filesystem worker is blocked.
            await asyncio.wait_for(asyncio.sleep(.01),.5)
            task.cancel();await asyncio.sleep(.01)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        path=await self.bridge.cached(identity)
        self.assertEqual(path.read_bytes(),b'data')
        self.assertEqual(list(self.bridge.root.glob('.incoming-*')),[])

    async def test_materialize_thread_cancel_joins_then_removes_owned_copy(self):
        source=self.root/'source';source.write_bytes(b'data')
        identity=shared.media_identity(1,2,3)
        await self.bridge.publish(identity,source)
        destination=self.root/'owned'
        started,release=threading.Event(),threading.Event()
        original=shared.shutil.copyfileobj
        def slow(*args):
            started.set();release.wait(3);return original(*args)
        with patch.object(shared.shutil,'copyfileobj',side_effect=slow):
            task=asyncio.create_task(self.bridge.materialize(identity,destination))
            self.assertTrue(await asyncio.to_thread(started.wait,1))
            task.cancel();await asyncio.sleep(.01)
            self.assertFalse(task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertFalse(destination.exists())
        self.assertEqual((await self.bridge.cached(identity)).read_bytes(),b'data')

    async def test_io_is_bounded_across_bridge_instances(self):
        release=threading.Event();two=threading.Event();lock=threading.Lock()
        state={'active':0,'peak':0}
        def blocked():
            with lock:
                state['active']+=1;state['peak']=max(state['peak'],state['active'])
                if state['active']==2:two.set()
            release.wait(3)
            with lock:state['active']-=1
        tasks=[asyncio.create_task(shared._io(blocked)) for _ in range(4)]
        self.assertTrue(await asyncio.to_thread(two.wait,1))
        await asyncio.sleep(.01);self.assertEqual(state['peak'],2)
        release.set();await asyncio.gather(*tasks)
        self.assertEqual(state['peak'],2)

    async def test_native_forward_large_media_never_downloads_or_reuploads(self):
        self.msg.peer_id=-1001
        self.msg.file.size=3_000_000_000
        client=NS(send_message=AsyncMock(return_value=NS(id=10)),forward_messages=AsyncMock(return_value=NS(id=11)))
        async def run(fn,**kw):return await fn()
        with patch.dict(os.environ,{'MEDIA_NATIVE_DELIVERY_IDENTITY':'user:123:native-v1'}):
            await shared.native_forward([self.msg],self.info,2,'header','new',client=client,run_api=run,extra={})
            await shared.native_forward([self.msg],self.info,2,'header','new',client=client,run_api=run,extra={})
        client.forward_messages.assert_awaited_once_with(2,self.msg,self.msg.peer_id)
        client.send_message.assert_awaited_once()
        self.msg.download_media.assert_not_awaited()
        self.assertFalse(self.bridge.root.exists())

    async def test_actual_main_native_wrapper_and_policy_keep_large_forward_enabled(self):
        self.msg.peer_id=-1001;self.msg.file.size=3_000_000_000
        client=NS(send_message=AsyncMock(return_value=NS(id=10)),forward_messages=AsyncMock(return_value=NS(id=11)))
        async def run(fn,**kw):return await fn()
        tree=ast.parse(Path('main.py').read_text())
        names={'direct_forward_large_media','should_direct_forward_large_media'}
        functions=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names]
        for fn in functions:
            fn.returns=None
            for a in fn.args.args:a.annotation=None
        env={'os':os,'client':client,'run_api':run,'native_forward':shared.native_forward,'errors':errors,
             'build_header_from_info':lambda *a,**kw:'header','get_media_size':lambda m:m.file.size,
             'media_type':lambda m:'video','log':lambda _:None,'tstamp':lambda:'now',
             'forward_cache_set':lambda *a:None,'should_direct_forward_large_media_mod':lambda *a,**kw:True,
             'FORWARDING_POLICY_CONFIG':object(),'forward_cache_get':lambda *a:True}
        exec(compile(ast.Module(body=functions,type_ignores=[]),'main.py','exec'),env)
        info=dict(self.info,chat_title='source',sender_username='sender',sender_display='Sender')
        with patch.dict(os.environ,{'MEDIA_SHARED_ENABLED':'true','MEDIA_NATIVE_DELIVERY_IDENTITY':'user:123:native-v1'}):
            self.assertTrue(env['should_direct_forward_large_media'](info,self.msg))
            for _ in range(2):await env['direct_forward_large_media'](self.msg,info,[{'name':'r','targets':[2]}],'new')
        client.forward_messages.assert_awaited_once()
        self.msg.download_media.assert_not_awaited()

    async def test_native_job_pipeline_uses_shared_revision_receipts(self):
        self.msg.peer_id=-1001
        client=NS(send_message=AsyncMock(return_value=NS(id=10)),forward_messages=AsyncMock(return_value=NS(id=11)))
        async def run(fn,**kw):return await fn()
        ctx=self.ctx();ctx.app_mode='listen';ctx.bot=None;ctx.client=client;ctx.run_api=run
        ctx.fetch_message_by_id=AsyncMock(return_value=self.msg)
        ctx.forward_cache_set=lambda *a:None
        ctx.route_map['r']['targets']=[2]
        info=dict(self.info,chat_title='source',sender_username='sender',sender_display='Sender')
        job={'info':info,'chat_id':info['chat_id'],'msg_id':17,'source_kind':'new','route_names':['r']}
        with patch.dict(os.environ,{'MEDIA_NATIVE_DELIVERY_IDENTITY':'user:123:native-v1'}):
            for _ in range(2):await job_processing.process_media_forward_job(job,ctx)
        client.forward_messages.assert_awaited_once()
        self.msg.download_media.assert_not_awaited()

    async def test_native_header_receipt_survives_definite_forward_rejection(self):
        self.msg.peer_id=-1001
        client=NS(send_message=AsyncMock(return_value=NS(id=10)),forward_messages=AsyncMock(side_effect=[errors.ChatForwardsRestrictedError(request=None),NS(id=11)]))
        async def run(fn,**kw):return await fn()
        with patch.dict(os.environ,{'MEDIA_NATIVE_DELIVERY_IDENTITY':'user:123:native-v1'}):
            with self.assertRaises(errors.ChatForwardsRestrictedError):
                await shared.native_forward([self.msg],self.info,2,'header','new',client=client,run_api=run,extra={})
            await shared.native_forward([self.msg],self.info,2,'header','new',client=client,run_api=run,extra={})
        client.send_message.assert_awaited_once()
        self.assertEqual(client.forward_messages.await_count,2)

    async def test_configuration_reads_environment_after_import(self):
        # Stop the per-test bridge mock to exercise the real lazy factory.
        self.factory.stop()
        with patch.dict(os.environ, {'MEDIA_SHARED_ENABLED':'false'},clear=True):
            self.assertIsNone(await shared.configured_bridge())
        with patch.dict(os.environ, {'MEDIA_SHARED_ENABLED':'true','MEDIA_SHARED_ROOT':str(self.root),
                'MEDIA_ACCOUNT_RATE_KEY':'after-load','REDIS_HOSTNAME':'fake-after-env','REDIS_USERNAME':'app'},clear=True), patch('redis.asyncio.Redis', return_value=self.redis) as constructor:
            bridge=await shared.configured_bridge()
            self.assertEqual(bridge.redis,self.redis)
            self.assertEqual(constructor.call_args.kwargs['host'],'fake-after-env')
            self.assertEqual(constructor.call_args.kwargs['username'],'app')

    async def test_notices_are_not_media_delivery_receipts(self):
        send=AsyncMock(return_value=NS(preserve_local_copy=True,delivery_kind='file_skip_notice'))
        await self.bridge.deliver('notice',send)
        self.assertIsNone(await self.bridge.receipt('notice'))
        with self.assertRaises(shared.AmbiguousDelivery):await self.bridge.deliver('notice',send)

    async def test_actual_legacy_run_api_records_floodwait_and_gates_retries(self):
        tree=ast.parse(Path('main.py').read_text())
        function=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='run_api')
        env={'Callable':object,'Any':object,'Optional':__import__('typing').Optional,'asyncio':NS(sleep=AsyncMock()),
             'errors':errors,'configured_bridge':AsyncMock(return_value=NS(wait=AsyncMock(),record_delay=AsyncMock())),
             'log':lambda _:None,'tstamp':lambda:'now'}
        # Remove type annotations only; execute the exact runtime wrapper body.
        function.returns=None
        for a in function.args.args:a.annotation=None
        exec(compile(ast.Module(body=[function],type_ignores=[]),'main.py','exec'),env)
        send=AsyncMock(side_effect=[errors.FloodWaitError(request=None,capture=2),'ok'])
        self.assertEqual(await env['run_api'](send,'fixture'),'ok')
        bridge=env['configured_bridge'].return_value
        self.assertEqual(bridge.wait.await_count,2)
        bridge.record_delay.assert_awaited_once_with(2)
        bridge.record_delay.reset_mock()
        send=AsyncMock(side_effect=[errors.FloodWaitError(request=None,capture=2),errors.FloodWaitError(request=None,capture=3)])
        with self.assertRaises(errors.FloodWaitError):await env['run_api'](send,'fixture')
        self.assertEqual([c.args[0] for c in bridge.record_delay.await_args_list],[2,3])


class DiscussionTests(unittest.IsolatedAsyncioTestCase):
    async def test_flag_strict_default_false(self):
        raw=[{'sources':[1],'targets':[2]}]
        self.assertFalse(load_routes(json.dumps(raw))[0]['include_discussion_media'])
        for value in ('true',1,None):
            raw[0]['include_discussion_media']=value
            with self.assertRaises(RuntimeError):load_routes(json.dumps(raw))

    async def test_real_resolver_only_sixty_discussion_media_no_second_main(self):
        from test_media_workbench import FakeClient, CHANNEL
        client=FakeClient()
        original=client.get_messages
        async def get(peer,ids):
            if isinstance(ids,list):return [await original(peer,i) for i in ids]
            return await original(peer,ids)
        client.get_messages=get
        tasks=[]; published=[]
        async def info(msg,**kw):return {'chat_id':msg.chat_id,'msg_id':msg.id,'sender_id':1}
        async def album(msgs,info,routes,kind):published.extend(m.id for m in msgs)
        worker=DiscussionSnapshots(client=client,spawn=lambda c:tasks.append(asyncio.create_task(c)),resolve_info=info,
            publish_media=AsyncMock(),publish_album=album,should_ignore=lambda i:False,should_skip=lambda m:False,log=lambda _:None)
        off=[{'name':'off','targets':[2]}]
        self.assertFalse(worker.schedule(client.main,off,'new'));self.assertFalse(tasks)
        routes=[{'name':'on','targets':[2],'include_discussion_media':True}]
        with patch('tg_forwarder.shared_media.configured_bridge',AsyncMock(return_value=None)),patch('tg_forwarder.control.source.utils.get_peer_id',lambda e:e.key):
            self.assertTrue(worker.schedule(client.main,routes,'new'))
            self.assertFalse(worker.schedule(client.main[1:],routes,'new'))
            await asyncio.gather(*tasks)
        self.assertEqual(len(published),60)
        self.assertEqual(len(set(published)),60)
        self.assertFalse(set(published)&{m.id for m in client.roots+client.main})
        self.assertFalse(worker.schedule(client.main,routes,'new'))
        self.assertFalse(worker.schedule(client.main,routes,'edited'))

if __name__=='__main__':unittest.main()
