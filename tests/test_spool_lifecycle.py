import asyncio
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch, AsyncMock
from tg_forwarder import spool_lifecycle as sl

class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'spool';sl.configure(self.root);self.m=sl.manager()
        self.addCleanup(lambda:[self.m.release(t) for t in list(self.m.leases)])
        self.addCleanup(sl._MANAGERS.pop,self.root,None)
        self.addCleanup(sl.configure,Path(self.tmp.name)/'unused')
    def acquired(self,safe=True):
        token,fd,path=self.m.allocate('new',-100,'photo.jpg');os.write(fd,b'data');os.close(fd)
        path=self.m.finalize(token,safe)
        return {'path':str(path),'spool_token':token}
    def job(self,count=1,targets=None,safe=True):
        snaps=[self.acquired(safe) for _ in range(count)]
        job={'source_kind':'new','route_names':['r'],'info':{},'spool_targets':targets or [['r',-2]],'snapshots':snaps}
        self.m.bind(job,job['spool_targets']);self.m.start(job,job['spool_targets'],True)
        return job
    def receipt(self,n=1):
        return NS(delivery_kind='file' if n==1 else 'album',sent_message_id=10,sent_message_ids=list(range(10,10+n)) if n>1 else None,preserve_local_copy=False)
    def deliver(self,job,target=-2):
        self.m.begin_target(job,'r',target)
        return self.m.target_result(job,'r',target,self.receipt(len(sl.snapshots(job))))
    def row(self,job):
        with self.m.db() as c:return self.m.row(c,sl.snapshots(job)[0]['spool_token'])
    def test_all_targets_durable_then_gc_receipts_survive(self):
        job=self.job(2,[['r',-2],['r',-3]])
        self.assertTrue(self.deliver(job));self.assertFalse(self.m.complete(job));self.assertFalse(self.m.finish(job,[s['path'] for s in sl.snapshots(job)],0))
        self.assertTrue(self.deliver(job,-3));self.assertTrue(self.m.complete(job))
        self.m.finish(job,[s['path'] for s in sl.snapshots(job)],0)
        result=self.m.scan();self.assertEqual(result['deleted'],2)
        self.assertTrue(self.m.complete(job));self.assertIsNotNone(self.m.begin_target(job,'r',-2))
        self.assertTrue(all(not Path(s['path']).exists() for s in sl.snapshots(job)))
    def test_partial_album_receipt_never_terminal(self):
        job=self.job(2);self.m.begin_target(job,'r',-2)
        self.assertFalse(self.m.target_result(job,'r',-2,self.receipt()))
        self.assertFalse(self.m.complete(job));self.m.scan(now=time.time()+10**6)
        self.assertTrue(Path(job['snapshots'][0]['path']).exists())
        with self.assertRaises(RuntimeError):self.m.begin_target(job,'r',-2)
    def test_crash_after_target_receipt_reconciles_without_journal(self):
        job=self.job();self.deliver(job)
        now=time.time();self.m.scan(now=now)
        self.assertEqual(self.row(job)['state'],'cleanup_pending')
        self.m.scan(now=now+86401);self.assertEqual(self.row(job)['state'],'deleted')
    def test_uncertain_publication_and_target_retained(self):
        job=self.job();self.m.begin_target(job,'r',-2)
        self.m.scan(now=time.time()+10**6)
        self.assertTrue(Path(job['snapshots'][0]['path']).exists())
        self.assertFalse(self.m.complete(job))
    def test_changed_inode_and_same_size_corruption_retained(self):
        for replace in (False,True):
            job=self.job();self.deliver(job);self.m.finish(job,[job['snapshots'][0]['path']],0)
            path=Path(job['snapshots'][0]['path']);old=path.stat()
            if replace:path.unlink()
            path.write_bytes(b'evil');os.utime(path,ns=(old.st_atime_ns,old.st_mtime_ns))
            result=self.m.scan();self.assertGreaterEqual(result['errors'],1);self.assertEqual(path.read_bytes(),b'evil')
    def test_symlink_and_hardlink_not_deleted(self):
        for mode in ('symlink','hardlink'):
            job=self.job();self.deliver(job);self.m.finish(job,[job['snapshots'][0]['path']],0)
            path=Path(job['snapshots'][0]['path']);other=self.root/(mode+'.jpg')
            if mode=='symlink':path.rename(other);path.symlink_to(other)
            else:os.link(path,other)
            self.m.scan();self.assertTrue(other.exists());self.assertTrue(path.exists())
    def test_unknown_aged_file_never_adopted(self):
        self.root.mkdir();unknown=self.root/'unknown';unknown.write_bytes(b'keep');os.utime(unknown,(1,1))
        result=self.m.scan(now=time.time()+10**6)
        self.assertEqual(unknown.read_bytes(),b'keep');self.assertFalse(result['unknown_walk'])
    def test_live_lease_vs_dead_unpublished(self):
        snap=self.acquired();token=snap['spool_token'];future=time.time()+73*3600
        self.m.scan(now=future);self.assertTrue(Path(snap['path']).exists())
        self.m.release(token);result=self.m.scan(now=future)
        self.assertEqual(result['abandoned'],1);self.assertFalse(Path(snap['path']).exists())
    def test_partial_acquisition_cleanup_and_failed_album_retention(self):
        token,fd,path=self.m.allocate('new',1,'part');os.write(fd,b'part');os.close(fd)
        self.m.acquisition_failed(token);self.assertFalse(path.exists())
        snap=self.acquired();self.m.retain_unpublished([snap['spool_token']],'partial-album')
        self.m.scan(now=time.time()+10**6);self.assertTrue(Path(snap['path']).exists())
    def test_shared_reference_not_cleanup_authority(self):
        job=self.job(safe=False);self.deliver(job);self.m.finish(job,[job['snapshots'][0]['path']],0)
        self.m.scan(now=time.time()+10**6);self.assertEqual(self.row(job)['state'],'retained')
    def test_file_and_byte_bounds_and_single_owner(self):
        jobs=[self.job() for _ in range(3)]
        for job in jobs:self.deliver(job);self.m.finish(job,[job['snapshots'][0]['path']],0)
        self.assertEqual(self.m.scan(limit=1)['deleted'],1)
        self.assertEqual(self.m.scan(max_bytes=3)['deleted'],0)
        fd=os.open(self.m.guard()/'gc.lock',os.O_RDWR);fcntl.flock(fd,fcntl.LOCK_EX)
        try:self.assertFalse(self.m.scan()['owner'])
        finally:os.close(fd)
    def test_missing_after_unlink_crash_is_idempotent(self):
        job=self.job();self.deliver(job);self.m.finish(job,[job['snapshots'][0]['path']],0)
        Path(job['snapshots'][0]['path']).unlink();self.m.scan()
        self.assertEqual(self.row(job)['state'],'cleanup_uncertain');self.assertTrue(self.m.complete(job))
    def test_root_symlink_and_missing_root_fail_closed(self):
        with self.assertRaises(ValueError):self.m.scan()
        self.root.symlink_to(self.tmp.name)
        with self.assertRaises(ValueError):self.m.allocate('new',1,'a')
    async def test_target_receipts_skip_transport_after_replay(self):
        job=self.job();send=AsyncMock(return_value=self.receipt())
        await sl.target_send(job,'r',-2,send);await sl.target_send(job,'r',-2,send)
        self.assertEqual(send.await_count,1)
    async def test_gc_worker_keeps_event_loop_responsive(self):
        self.root.mkdir();ticks=0;logs=[]
        original=self.m.scan
        def slow():time.sleep(.08);return original()
        with patch.object(self.m,'scan',side_effect=slow):
            task=asyncio.create_task(sl.gc_loop(logs.append,100))
            for _ in range(8):await asyncio.sleep(.01);ticks+=1
            for _ in range(100):
                if logs:break
                await asyncio.sleep(.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(ticks,8);self.assertEqual(len(logs),1)
    async def test_broker_sees_durable_intent_before_publication(self):
        from tg_forwarder.processing.kafka_jobs import kafka_send
        snap=self.acquired();job={'source_kind':'new','route_names':['r'],'snapshot':snap,'spool_targets':[['r',-2]]}
        async def publish(topic,data):
            decoded=json.loads(data);self.assertTrue(decoded['spool_ref'])
            with self.m.db() as c:self.assertIsNotNone(c.execute('SELECT 1 FROM refs WHERE ref=?',(decoded['spool_ref'],)).fetchone())
        await kafka_send('media',job,NS(send_and_wait=publish),lambda x:json.dumps(x).encode(),lambda x:None)
        self.assertEqual(self.row(job)['state'],'referenced')

    async def test_cancel_joins_download_before_owned_partial_cleanup(self):
        from tg_forwarder.processing import snapshot
        entered=asyncio.Event();joined=asyncio.Event()
        async def download(stream):
            stream.write(b'part');entered.set()
            try:await asyncio.sleep(100)
            finally:await asyncio.sleep(.01);joined.set()
        msg=NS(id=1,photo=NS(id=3),document=None,media=True,file=NS(size=4,mime_type='image/jpeg'),text='',message='',download_media=download)
        async def run(fn,**kw):return await fn()
        with patch.object(snapshot,'configured_bridge',AsyncMock(return_value=None)):
            task=asyncio.create_task(snapshot.snapshot_media_message(msg,{'chat_id':1},'new',self.root,asyncio.Semaphore(1),run,lambda _:None))
            await entered.wait();task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(joined.is_set())
        with self.m.db() as c:
            rows=[json.loads(r[0]) for r in c.execute('SELECT data FROM files')]
        self.assertEqual([r['state'] for r in rows],['deleted'])
        self.assertFalse((self.root/rows[0]['part']).exists())
    async def test_partial_native_album_is_retained_not_published(self):
        from tg_forwarder.processing import snapshot
        snap=self.acquired()
        with patch.object(snapshot,'snapshot_media_message',AsyncMock(side_effect=[snap,RuntimeError('IO')])):
            with self.assertRaises(RuntimeError):
                await snapshot.snapshot_album_messages([NS(id=1),NS(id=2)],{'chat_id':1},'new',self.root,None,None,lambda _:None)
        with self.m.db() as c:self.assertEqual(self.m.row(c,snap['spool_token'])['state'],'failed_recoverable')
    def test_unlink_failure_retries_without_resend_or_receipt_loss(self):
        job=self.job();self.deliver(job);self.m.finish(job,[job['snapshots'][0]['path']],0)
        with patch.object(os,'unlink',side_effect=PermissionError('busy')):
            result=self.m.scan();self.assertEqual(result['errors'],1)
        self.assertTrue(self.m.complete(job))
        claims=list((self.m.guard()/'quarantine').glob(job['snapshots'][0]['spool_token']+'-*/payload'))
        self.assertEqual(len(claims),1);self.assertEqual(claims[0].read_bytes(),b'data')
        self.m.scan(now=time.time()+3601);self.assertEqual(self.row(job)['state'],'deleted')

    async def test_native_file_job_partial_target_failure_retains_and_no_resend(self):
        from tg_forwarder.processing import job_processing as jp
        job=self.job(targets=[['r',-2],['r',-3]])
        job['snapshot']=job.pop('snapshots')[0]
        job['info']={'chat_id':1,'msg_id':1};job['snapshot']['caption_text']=''
        sender=AsyncMock(side_effect=[self.receipt(),False])
        ctx=NS(route_map={'r':{'name':'r','targets':[-2,-3]}},delete_after_send=True,
               should_ignore=lambda *a:False,ignore_users=set(),ignore_ids=set(),
               build_header_from_info=lambda *a,**k:'header',is_stale_file=lambda p:False,
               media_target_parallel=False,send_file_to_target=sender,log=lambda _:None)
        async def legacy(send,*args):return await send()
        with patch.object(jp,'legacy_send',side_effect=legacy),patch.object(jp,'store_message_mapping',AsyncMock()):
            with self.assertRaises(RuntimeError):await jp.process_media_file_job(job,ctx)
            with self.assertRaises(RuntimeError):await jp.process_media_file_job(job,ctx)
        self.assertEqual(sender.await_count,2);self.assertFalse(self.m.complete(job))
        self.m.scan(now=time.time()+10**6);self.assertTrue(Path(job['snapshot']['path']).exists())

    def test_replacement_after_source_stat_never_unlinks_foreign_any_cleanup_lane(self):
        for lane in ('terminal','acquisition','abandoned'):
            for kind in ('file','symlink','hardlink'):
                if lane=='acquisition':
                    token,fd,path=self.m.allocate('new',1,'partial');os.write(fd,b'owned');os.close(fd)
                    with self.m.db() as c:row=self.m.row(c,token)
                    invoke=lambda:self.m.acquisition_failed(token)
                else:
                    job=self.job() if lane=='terminal' else {'snapshots':[self.acquired()]}
                    row=self.row(job);token=row['token'];path=Path(job['snapshots'][0]['path'])
                    if lane=='terminal':
                        self.deliver(job);self.m.finish(job,[str(path)],0)
                        invoke=self.m.scan
                    else:
                        self.m.release(token);invoke=lambda:self.m.scan(now=time.time()+73*3600)
                aside=path.with_name(path.name+'.original');victim=self.root/(token+'.foreign');victim.write_bytes(b'FOREIGN')
                original_stat=os.stat;fired=[]
                def raced(name,*args,**kw):
                    info=original_stat(name,*args,**kw)
                    if not fired and name==path.name and kw.get('dir_fd') is not None and kw.get('follow_symlinks') is False:
                        fired.append(True);path.rename(aside)
                        if kind=='file':path.write_bytes(b'FOREIGN')
                        elif kind=='symlink':path.symlink_to(victim)
                        else:os.link(victim,path)
                    return info
                with patch.object(os,'stat',side_effect=raced):invoke()
                self.assertTrue(fired,(lane,kind));self.assertTrue(aside.exists())
                self.assertEqual(victim.read_bytes(),b'FOREIGN')
                with self.m.db() as c:current=self.m.row(c,token)
                self.assertNotEqual(current['state'],'deleted',(lane,kind))
                claims=list((self.m.guard()/'quarantine').glob(token+'-*/intent.json'))
                uncertain=[json.loads(p.read_text()) for p in claims if json.loads(p.read_text()).get('state')=='uncertain']
                self.assertEqual(len(uncertain),1,(lane,kind))
                payload=Path(uncertain[0]['quarantine']);self.assertTrue(payload.is_symlink() or payload.exists())
                self.assertEqual(payload.read_bytes(),b'FOREIGN')
    def test_crash_after_move_before_verification_restarts_from_private_claim(self):
        job=self.job();self.deliver(job);path=Path(job['snapshots'][0]['path']);self.m.finish(job,[str(path)],0)
        original_stat=os.stat;crashed=[]
        def crash(name,*args,**kw):
            if name=='payload' and kw.get('dir_fd') is not None and not crashed:
                crashed.append(True);raise RuntimeError('simulated process death after atomic claim')
            return original_stat(name,*args,**kw)
        with patch.object(os,'stat',side_effect=crash):
            with self.assertRaises(RuntimeError):self.m.scan()
        self.assertFalse(path.exists());self.assertEqual(self.row(job)['state'],'cleanup_claimed')
        replacement=path;replacement.write_bytes(b'NEW-FOREIGN')
        self.m.scan();self.assertEqual(replacement.read_bytes(),b'NEW-FOREIGN')
        self.assertEqual(self.row(job)['state'],'deleted');self.assertTrue(self.m.complete(job))
    def test_crash_after_private_unlink_replays_receipt_without_source_unlink(self):
        job=self.job();self.deliver(job);path=Path(job['snapshots'][0]['path']);self.m.finish(job,[str(path)],0)
        original_put=self.m.put
        def crash(c,row):
            if row['state']=='deleted':raise RuntimeError('simulated DB commit loss')
            return original_put(c,row)
        with patch.object(self.m,'put',side_effect=crash):
            with self.assertRaises(RuntimeError):self.m.scan()
        path.write_bytes(b'FOREIGN-AFTER-UNLINK')
        self.m.scan();self.assertEqual(path.read_bytes(),b'FOREIGN-AFTER-UNLINK');self.assertTrue(self.m.complete(job))
    def test_source_ancestor_replacement_cannot_redirect_claim_into_foreign_directory(self):
        job=self.job();self.deliver(job);path=Path(job['snapshots'][0]['path']);self.m.finish(job,[str(path)],0)
        parent=path.parent;aside=parent.with_name(parent.name+'-aside');foreign=self.root/'foreign-directory';foreign.mkdir();(foreign/path.name).write_bytes(b'FOREIGN')
        original_stat=os.stat;fired=[]
        def race(name,*args,**kw):
            info=original_stat(name,*args,**kw)
            if not fired and name==path.name and kw.get('dir_fd') is not None:
                fired.append(True);parent.rename(aside);parent.symlink_to(foreign)
            return info
        with patch.object(os,'stat',side_effect=race):self.m.scan()
        self.assertEqual((foreign/path.name).read_bytes(),b'FOREIGN')

    def test_quarantine_destination_collision_never_overwrites_foreign(self):
        job=self.job();self.deliver(job);path=Path(job['snapshots'][0]['path']);self.m.finish(job,[str(path)],0)
        original=sl.rename_exclusive;fired=[]
        def collide(source,sfd,dest,dfd):
            if dest=='payload' and not fired:
                fired.append(True);fd=os.open(dest,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600,dir_fd=dfd);os.write(fd,b'FOREIGN');os.close(fd)
            return original(source,sfd,dest,dfd)
        with patch.object(sl,'rename_exclusive',side_effect=collide):self.m.scan()
        self.assertEqual(path.read_bytes(),b'data')
        payload=list((self.m.guard()/'quarantine').glob(job['snapshots'][0]['spool_token']+'-*/payload'))[0]
        self.assertEqual(payload.read_bytes(),b'FOREIGN');self.assertNotEqual(self.row(job)['state'],'deleted')
    def test_changed_verified_claim_retained_on_retry(self):
        job=self.job();self.deliver(job);self.m.finish(job,[job['snapshots'][0]['path']],0)
        with patch.object(os,'unlink',side_effect=PermissionError('busy')):self.m.scan()
        payload=list((self.m.guard()/'quarantine').glob(job['snapshots'][0]['spool_token']+'-*/payload'))[0]
        before=payload.stat();payload.write_bytes(b'evil');os.utime(payload,ns=(before.st_atime_ns,before.st_mtime_ns))
        self.m.scan(now=time.time()+3601);self.assertEqual(payload.read_bytes(),b'evil');self.assertEqual(self.row(job)['state'],'cleanup_uncertain')
    def test_private_claim_permissions_changed_before_unlink_retains_payload(self):
        job=self.job();self.deliver(job);self.m.finish(job,[job['snapshots'][0]['path']],0)
        original_stat=os.stat;fired=[]
        def race(name,*args,**kw):
            info=original_stat(name,*args,**kw)
            if name=='payload' and not fired:
                fired.append(True);os.fchmod(kw['dir_fd'],0o777)
            return info
        with patch.object(os,'stat',side_effect=race):self.m.scan()
        payload=list((self.m.guard()/'quarantine').glob(job['snapshots'][0]['spool_token']+'-*/payload'))[0]
        self.assertEqual(payload.read_bytes(),b'data');self.assertNotEqual(self.row(job)['state'],'deleted')

    def test_dead_partial_actual_bytes_respect_gc_budget(self):
        token,fd,path=self.m.allocate('new',1,'large-partial');os.write(fd,b'12345678');os.close(fd);self.m.release(token)
        result=self.m.scan(now=time.time()+73*3600,max_bytes=4)
        self.assertEqual(result['bytes'],0);self.assertTrue(path.exists())
        with self.m.db() as c:self.assertNotEqual(self.m.row(c,token)['state'],'deleted')

if __name__=='__main__':unittest.main()
