"""Offline actual publisher/consumer regressions for the four release blockers."""
import asyncio
import copy
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, Mock, patch
from test_future_comments import extract, Redis, SOURCE, GROUP
import test_future_comments as collector_tests
from tg_forwarder.processing import future_delivery as delivery
from tg_forwarder.processing import kafka_jobs, job_processing
from tg_forwarder.shared_media import SharedMedia, media_identity
from tg_forwarder.runtime.bot_runtime import SendOutcome


class Consumer:
    def __init__(self, jobs, topic):
        self.records=[NS(value=j if isinstance(j,bytes) else json.dumps(j).encode(),topic=topic,partition=0,offset=i) for i,j in enumerate(jobs)]
        self.start=AsyncMock();self.stop=AsyncMock();self.marks=[];self.commits=[]
    async def __aiter__(self):
        for record in self.records:yield record


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.redis=Redis();self.topic='media-future-comments'
        self.route=dict(name='existing',sources={SOURCE},targets=[2])
        self.bridge=SharedMedia(self.redis,Path(self.tmp.name)/'cache',account_rate_key='offline',min_free_bytes=0)
        self.bridge.wait=AsyncMock()
        for name in ('tg_forwarder.processing.future_delivery.get_redis','tg_forwarder.processing.future_delivery.configured_bridge','tg_forwarder.shared_media.configured_bridge'):
            result=self.redis if name.endswith('get_redis') else self.bridge
            p=patch(name,AsyncMock(return_value=result));p.start();self.addCleanup(p.stop)
        for name in ('store_message_mapping','store_album_mapping'):
            p=patch('tg_forwarder.processing.job_processing.'+name,AsyncMock());p.start();self.addCleanup(p.stop)
        p=patch.dict(os.environ,MEDIA_DELIVERY_IDENTITY='bot:offline',FORUM_TOPIC_DELIVERY_MODE='mirror');p.start();self.addCleanup(p.stop)
        self.send=AsyncMock(return_value=SendOutcome(True,sent_message_id=101,sent_message_ids=[101,102],delivery_kind='album',general_message_ids=[101,102]))
        self.ctx=NS(route_map={'existing':self.route},should_ignore=lambda *a:False,ignore_users=set(),ignore_ids=set(),
            build_header_from_info=lambda *a,**kw:'header',is_stale_file=lambda p:False,
            media_target_parallel=False,delete_after_send=True,send_album_to_target=self.send,send_file_to_target=self.send,log=lambda x:None)
        self.logs=[]

    async def job(self,n=2):
        values=[]
        for mid in range(1,n+1):
            path=Path(self.tmp.name)/str(mid);path.write_bytes(b'photo'+bytes([mid]))
            values.append(dict(path=str(path),media_type='photo',msg_id=mid,caption_text='caption',shared_identity=media_identity(GROUP,mid,70000+mid)))
        context=dict(source=SOURCE,generation=1,signature='routes',expected=[v['shared_identity'] for v in values])
        job=dict(job_type='media_album_file' if n>1 else 'media_file',source_kind='discussion',route_names=['existing'],info=dict(chat_id=GROUP,msg_id=1,sender_id=9,future_comments=context))
        job.update({'snapshots':values} if n>1 else {'snapshot':values[0]})
        await delivery.seal(job)
        await self.redis.set(delivery.evidence_key(job),json.dumps({'status':'prepared','job':{'topic':self.topic,'data':json.dumps(job)}}))
        return job

    def environment(self,normal,*,processing=False):
        def batcher(consumer,lane):
            async def commit(**kw):consumer.commits.append((lane,len(consumer.marks)))
            return NS(mark_processed=lambda r:consumer.marks.append(r.offset),maybe_commit=commit)
        async def dispatch(job,ctx):
            if job.get('ordinary'):return []
            return await job_processing.dispatch_media_job(job,ctx)
        env=dict(media_consumer=normal,OffsetCommitBatcher=batcher,json=json,maybe_debug_log=lambda *a:None,
            log=self.logs.append,tstamp=lambda:'now',sleep_until_due=AsyncMock(),future_comment_job_permitted=AsyncMock(return_value=True),
            job_fingerprint=lambda j:'fingerprint',journal_get=lambda f:{'status':'processing'} if processing else None,
            journal_claim=lambda *a:True,journal_mark=Mock(),journal_record_failure=lambda *a:1,
            SEND_JOURNAL_MAX_FAILURES=5,dispatch_media_job=AsyncMock(side_effect=dispatch),build_job_processing_ctx=lambda:self.ctx,
            cleanup_files=lambda *a:None)
        extract('media_consumer_loop',env)
        return env

    async def test_bad_future_does_not_stop_ordinary_and_commit_sets_independent(self):
        job=await self.job();Path(job['snapshots'][0]['path']).unlink()
        future=Consumer([job],self.topic);normal=Consumer([dict(ordinary=True,info={})],'media')
        env=self.environment(normal)
        env.update(asyncio=asyncio,get_redis=AsyncMock(return_value=self.redis),ROUTES=[{'forward_future_comment_photos':True}],
                   KAFKA_MEDIA_TOPIC='media',KAFKA_CONSUMER_GROUP='original',KAFKA_BOOTSTRAP_SERVERS='offline',
                   AIOKafkaConsumer=lambda *a,**kw:future)
        metadata=NS(topics=lambda:{self.topic},partitions_for_topic=lambda t:{0})
        probe=NS(bootstrap=AsyncMock(),close=AsyncMock(),fetch_all_metadata=AsyncMock(return_value=metadata))
        supervisor=extract('future_consumer_supervisor',env)
        with patch('aiokafka.client.AIOKafkaClient',return_value=probe):
            await asyncio.gather(supervisor(),env['media_consumer_loop']())
        self.assertEqual(normal.marks,[0]);self.assertEqual(future.marks,[])
        self.assertTrue(all(lane=='media' for lane,_ in normal.commits))
        self.assertTrue(all(lane=='future-comments' for lane,_ in future.commits))
        self.assertIsNotNone(await self.redis.get(delivery.BLOCKED));self.send.assert_not_awaited()
        self.assertEqual(json.loads(await self.redis.get(delivery.BLOCKED))['offset'],0)
        probe.close.assert_awaited_once()

    async def test_malformed_future_payload_is_lane_local(self):
        future=Consumer([b'{invalid'],'future');normal=Consumer([dict(ordinary=True,info={})],'media')
        env=self.environment(normal)
        async def future_run():
            with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
        await asyncio.gather(future_run(),env['media_consumer_loop']())
        self.assertEqual(normal.marks,[0]);self.assertFalse(future.marks)
        self.assertIsNotNone(await self.redis.get(delivery.BLOCKED))

    async def test_processing_journal_restart_never_dispatches_or_commits(self):
        job=await self.job();future=Consumer([job],self.topic);env=self.environment(None,processing=True)
        with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
        env['dispatch_media_job'].assert_not_awaited();self.assertFalse(future.marks)
        self.assertIn('job',json.loads(await self.redis.get(delivery.evidence_key(job))))

    async def test_inflight_evidence_without_journal_never_auto_resends(self):
        job=await self.job();await delivery.terminal(job,'in_flight')
        future=Consumer([job],self.topic);env=self.environment(None)
        with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
        env['dispatch_media_job'].assert_not_awaited();self.assertFalse(future.marks)

    async def test_done_journal_without_receipts_is_not_delivery_proof(self):
        job=await self.job();future=Consumer([job],self.topic);env=self.environment(None)
        env['journal_get']=lambda f:{'status':'done'}
        with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
        self.assertFalse(future.marks);self.send.assert_not_awaited()

    async def test_all_or_partial_missing_album_never_transport_or_done(self):
        for missing in (1,2):
            self.redis.data.clear();job=await self.job()
            for item in job['snapshots'][:missing]:Path(item['path']).unlink()
            future=Consumer([job],self.topic);env=self.environment(None)
            with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
            self.assertFalse(future.marks);env['journal_mark'].assert_not_called();self.send.assert_not_awaited()

    async def test_changed_same_size_file_and_symlink_rejected_before_transport(self):
        job=await self.job();path=Path(job['snapshots'][0]['path']);path.write_bytes(b'xxxxxx')
        with self.assertRaises(ValueError):await job_processing.dispatch_media_job(job,self.ctx)
        path.unlink();path.symlink_to(job['snapshots'][1]['path'])
        with self.assertRaises(ValueError):await job_processing.dispatch_media_job(job,self.ctx)
        self.send.assert_not_awaited()

    async def test_success_requires_all_target_receipts_and_keeps_prepared_evidence(self):
        job=await self.job();future=Consumer([job],self.topic);env=self.environment(None)
        await env['media_consumer_loop'](future,'future-comments')
        self.assertEqual(future.marks,[0]);self.send.assert_awaited_once()
        evidence=json.loads(await self.redis.get(delivery.evidence_key(job)))
        self.assertEqual(evidence['status'],'complete');self.assertEqual(json.loads(evidence['job']['data']),job)

    async def test_partial_album_receipt_is_not_complete(self):
        job=await self.job();self.send.return_value=SendOutcome(True,sent_message_id=101,sent_message_ids=[101],delivery_kind='album')
        future=Consumer([job],self.topic);env=self.environment(None)
        with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
        self.assertFalse(future.marks);env['journal_mark'].assert_not_called()

    async def test_empty_dispatch_is_not_success(self):
        job=await self.job();future=Consumer([job],self.topic);env=self.environment(None)
        env['dispatch_media_job']=AsyncMock(return_value=[])
        with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
        self.assertFalse(future.marks);env['journal_mark'].assert_not_called()

    async def test_payload_must_match_durable_prepared_bytes(self):
        job=await self.job();job['snapshots'][0]['caption_text']='tampered'
        future=Consumer([job],self.topic);env=self.environment(None)
        with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
        self.send.assert_not_awaited();self.assertFalse(future.marks)

    async def test_default_off_no_future_tasks_consumer_or_client(self):
        env=dict(json=json,log=self.logs.append,ROUTES=[],edit_mapping_enabled=lambda:False,spawn_bg=lambda c:self.fail('unexpected future task'),
                 _FUTURE_COMMENTS=None,get_redis=AsyncMock(side_effect=AssertionError('no Redis configured')))
        await extract('start_future_consumer',env)();await extract('start_future_comments',env)()
        env['edit_mapping_enabled']=lambda:True;env['get_redis']=AsyncMock(return_value=self.redis)
        await env['start_future_consumer']();await env['start_future_comments']()

    async def test_preflight_all_topics_only_and_absent_refuses(self):
        client=NS(fetch_all_metadata=AsyncMock(return_value=NS(topics=lambda:set(),partitions_for_topic=lambda t:None)))
        with self.assertRaises(RuntimeError):await delivery.metadata_ready(client,self.topic)
        client.fetch_all_metadata.assert_awaited_once_with()


    async def test_confirmed_receipts_recover_processing_without_resend(self):
        job=await self.job();await job_processing.dispatch_media_job(copy.deepcopy(job),self.ctx)
        self.send.reset_mock()
        future=Consumer([job],self.topic);env=self.environment(None,processing=True)
        await env['media_consumer_loop'](future,'future-comments')
        self.assertEqual(future.marks,[0]);self.send.assert_not_awaited()
        env['dispatch_media_job'].assert_not_awaited()

    async def test_one_target_missing_receipt_prevents_whole_job_completion(self):
        job=await self.job();self.route['targets']=[2,3]
        async def send(route,target,*args):
            if target==3:return SendOutcome(False)
            return SendOutcome(True,sent_message_id=101,sent_message_ids=[101,102],delivery_kind='album')
        self.send.side_effect=send
        future=Consumer([job],self.topic);env=self.environment(None)
        with self.assertRaises(RuntimeError):await env['media_consumer_loop'](future,'future-comments')
        self.assertFalse(future.marks);env['journal_mark'].assert_not_called()
        self.assertTrue(all(Path(s['path']).exists() for s in job['snapshots']))

    async def test_canceled_old_selection_drains_without_transport_or_proof_files(self):
        job=await self.job();await delivery.terminal(job,'canceled')
        for item in job['snapshots']:Path(item['path']).unlink()
        future=Consumer([job],self.topic);env=self.environment(None)
        await env['media_consumer_loop'](future,'future-comments')
        self.assertEqual(future.marks,[0]);self.send.assert_not_awaited()
        self.assertEqual(json.loads(await self.redis.get(delivery.evidence_key(job)))['status'],'canceled')


    async def cancellation_race(self, finish):
        from tg_forwarder.processing.future_comments import FutureComments
        job=await self.job();key=delivery.evidence_key(job)
        prepared=json.loads(await self.redis.get(key))['job']
        state={'pending':{'evidence_key':key,'job':prepared},'evidence':[]}
        worker=object.__new__(FutureComments)
        read=asyncio.Event();resume=asyncio.Event();paused=False
        async def hook(k,old,new):
            nonlocal paused
            old_value,new_value=json.loads(old),json.loads(new)
            if not paused and old_value['status']=='prepared' and new_value['status']=='canceled':
                paused=True;read.set();await resume.wait()
        self.redis.before_evidence_cas=hook
        cancel=asyncio.create_task(worker.preserve_pending(self.redis,state))
        await asyncio.wait_for(read.wait(),1)
        handoff=await delivery.terminal(job,'in_flight')
        self.assertTrue(handoff['accepted'])
        if finish:await delivery.terminal(job,'complete')
        resume.set();await asyncio.wait_for(cancel,1)
        value=json.loads(await self.redis.get(key))
        self.assertEqual(value['job'],prepared)
        self.assertEqual(value['status'],'complete' if finish else 'in_flight')
        self.assertEqual(value['previous_status'],'in_flight' if finish else 'prepared')
        self.assertIn(key,state['evidence'])
        return job,value

    async def test_stale_cancel_observes_latest_inflight_and_retains_uncertainty(self):
        job,value=await self.cancellation_race(False)
        self.assertTrue(value['cancel_requested'])
        again=await delivery.terminal(job,'in_flight')
        self.assertFalse(again['accepted']);self.assertEqual(again['status'],'in_flight')
        completed=await delivery.terminal(job,'complete')
        self.assertEqual(completed['status'],'complete');self.assertTrue(completed['value']['cancel_requested'])

    async def test_stale_cancel_cannot_downgrade_confirmed_complete(self):
        job,value=await self.cancellation_race(True)
        key=delivery.evidence_key(job);before=await self.redis.get(key)
        await delivery.terminal(job,'canceled');await delivery.terminal(job,'complete')
        self.assertEqual(await self.redis.get(key),before)

    async def test_cancel_wins_after_consumer_read_before_handoff_no_transport(self):
        job=await self.job();future=Consumer([job],self.topic);env=self.environment(None)
        original=delivery.terminal
        async def cancel_then_handoff(j,status):
            if status=='in_flight':await original(j,'canceled')
            return await original(j,status)
        with patch.object(delivery,'terminal',side_effect=cancel_then_handoff):
            await env['media_consumer_loop'](future,'future-comments')
        self.assertEqual(future.marks,[0]);env['dispatch_media_job'].assert_not_awaited()
        self.send.assert_not_awaited()
        self.assertEqual(json.loads(await self.redis.get(delivery.evidence_key(job)))['status'],'canceled')

    async def test_canceled_handoff_and_repeated_cancel_are_idempotent(self):
        job=await self.job();key=delivery.evidence_key(job)
        await delivery.terminal(job,'canceled');before=await self.redis.get(key)
        await delivery.terminal(job,'canceled')
        result=await delivery.terminal(job,'in_flight')
        self.assertFalse(result['accepted']);self.assertEqual(result['status'],'canceled')
        self.assertEqual(await self.redis.get(key),before)

    async def test_cas_rejects_immutable_job_payload_change_during_race(self):
        job=await self.job();key=delivery.evidence_key(job);changed=False
        async def hook(k,old,new):
            nonlocal changed
            if not changed:
                changed=True;value=json.loads(old);value['job']['data']='changed'
                self.redis.data[k]=json.dumps(value)
        self.redis.before_evidence_cas=hook
        with self.assertRaisesRegex(RuntimeError,'payload changed'):
            await delivery.terminal(job,'in_flight')
        self.assertEqual(json.loads(await self.redis.get(key))['status'],'prepared')


class PublicationTests(unittest.IsolatedAsyncioTestCase):
    setUp=collector_tests.FutureTests.setUp
    state=collector_tests.FutureTests.state
    enable=collector_tests.FutureTests.enable
    add=collector_tests.FutureTests.add
    payload=collector_tests.FutureTests.payload
    fake_publish=collector_tests.FutureTests.fake_publish
    fake_media=collector_tests.FutureTests.fake_media
    fake_album=collector_tests.FutureTests.fake_album
    async def test_snapshot_none_actual_publisher_never_advances_or_enqueues(self):
        await self.enable();self.add(2)
        actual=NS(client=object(),send_and_wait=AsyncMock())
        async def publish(msg,info,routes,kind):
            from tg_forwarder.processing.future_comments import OutboxProducer
            await kafka_jobs.publish_media_job(msg,info,routes,kind,media_delay_seconds=0,topic='media',producer=OutboxProducer(actual),
                json_bytes=lambda j:json.dumps(j).encode(),log=lambda x:None,should_direct_forward_large_media=lambda *a:False,
                snapshot_kwargs=dict(spool_dir=Path(self.tmp.name),download_semaphore=None,run_api=None))
        self.worker.publish_media=publish
        with patch.object(kafka_jobs,'snapshot_media_message',AsyncMock(return_value=None)):
            await self.worker.tick()
        self.assertEqual((await self.state())['cursor'],1);actual.send_and_wait.assert_not_awaited()
        self.assertTrue((await self.state())['pending'])

    async def test_partial_album_actual_publisher_never_sends_subset(self):
        await self.enable();self.add(2,album=77);self.add(3,album=77)
        actual=NS(client=object(),send_and_wait=AsyncMock())
        async def publish(msgs,info,routes,kind):
            from tg_forwarder.processing.future_comments import OutboxProducer
            subset=self.payload([msgs[0]],info,routes)['snapshots']
            with patch.object(kafka_jobs,'snapshot_album_messages',AsyncMock(return_value=subset)):
                await kafka_jobs.publish_album_job(msgs,info,routes,kind,media_delay_seconds=0,topic='media',producer=OutboxProducer(actual),
                    json_bytes=lambda j:json.dumps(j).encode(),log=lambda x:None,should_direct_forward_large_album=lambda *a:False,
                    snapshot_kwargs=dict(spool_dir=Path(self.tmp.name),download_semaphore=None,run_api=None))
        self.worker.publish_album=publish
        await self.worker.tick();self.now+=4;await self.worker.tick()
        self.assertEqual((await self.state())['cursor'],1);actual.send_and_wait.assert_not_awaited()
        self.assertEqual((await self.state())['pending']['ids'],[2,3])

    async def test_acked_delivery_survives_restart_and_disable_preserves_exact_evidence(self):
        from tg_forwarder.processing.future_comments import FutureComments, FLAG
        from tg_forwarder.processing.future_delivery import evidence_key
        await self.enable();self.add(2)
        # Broker acknowledges publication, but no sender receipt/completion yet.
        actual=NS(client=object(),send_and_wait=AsyncMock())
        async def publish(msg,info,routes,kind):
            from tg_forwarder.processing.future_comments import OutboxProducer
            await OutboxProducer(actual).send_and_wait('media',json.dumps(self.payload([msg],info,routes)).encode())
        self.worker.publish_media=publish;await self.worker.tick();state=await self.state()
        key=state['pending']['evidence_key'];prepared=json.loads(await self.redis.get(key))['job']
        self.add(3);self.worker=FutureComments(**self.kw);await self.worker.tick();self.media.assert_not_awaited()
        self.routes[0][FLAG]=False;await self.worker.tick()
        evidence=json.loads(await self.redis.get(key))
        self.assertEqual(evidence['status'],'canceled');self.assertEqual(evidence['job'],prepared)
        self.assertIn(key,(await self.state())['evidence'])

    async def test_completed_evidence_retention_is_bounded_without_stopping_continuous_collection(self):
        await self.enable()
        for mid in range(2,37):
            self.add(mid)
            for _ in range(4):await self.worker.tick()
            self.assertEqual((await self.state())['cursor'],mid)
        state=await self.state();self.assertLessEqual(len(state.get('evidence',[])),32)
        records=[k for k in self.redis.data if k.startswith(delivery.PREFIX+'delivery:')]
        self.assertLessEqual(len(records),33)

    async def test_blocked_lane_does_not_collect_but_disable_still_fences(self):
        from tg_forwarder.processing.future_comments import FLAG
        await self.enable();self.add(2)
        await self.redis.set(delivery.BLOCKED,'{"reason":"uncertain"}')
        before=len(self.client.calls);await self.worker.tick()
        self.assertEqual(len(self.client.calls),before);self.media.assert_not_awaited()
        self.routes[0][FLAG]=False;await self.worker.tick()
        self.assertFalse((await self.state())['active'])
