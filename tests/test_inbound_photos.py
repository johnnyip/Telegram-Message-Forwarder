import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import struct
import threading
import errno
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace as NS
from telegram import Update
from telegram.error import RetryAfter, BadRequest
import test_cached_resend as fixtures
from test_shared_media import Redis
from tg_forwarder.canonical_media import photo_unique_id, secure_path, trusted_file_owner_uids
from tg_forwarder.control.inbound import payload_for, photo_id_from_file_id, CLOUD_DOWNLOAD_LIMIT, OWNER
from tg_forwarder.control.recover_inbound import recover
from tg_forwarder.control.store import Store
from tg_forwarder.shared_media import SharedMedia

BOT = 6317800517
PHOTO = 6154650511710734502


def rle(raw):
    out=bytearray(); n=0
    while n<len(raw):
        if raw[n]:out.append(raw[n]);n+=1
        else:
            end=n
            while end<len(raw) and not raw[end] and end-n<255:end+=1
            out.extend((0,end-n));n=end
    return base64.urlsafe_b64encode(out).decode().rstrip('=')


def file_id(major=4, reference=b''):  # independent encoder fixture from documented pinned wire fields
    raw=struct.pack('<ii',2 | ((1<<25) if reference else 0),2)
    if reference:
        raw+=bytes([len(reference)])+reference+b'\0'*((-(len(reference)+1))%4)
    raw+=struct.pack('<qqq',PHOTO,12345,98765)
    if major==4:raw+=struct.pack('<iiii',1,2,ord('y'),33)+bytes([30,4])
    else:raw+=struct.pack('<qi',1234,33)+bytes([2])
    return rle(raw)


def raw_update(mid=73263, uid=OWNER, origin='user', group='actual-album'):
    message={'message_id':mid,'date':1794590000,'chat':{'id':uid,'type':'private'},
        'from':{'id':uid,'is_bot':False,'first_name':'owner'},
        'photo':[{'file_id':'small','file_unique_id':'small','width':10,'height':10,'file_size':1},
                 {'file_id':file_id(reference=b'reference'),'file_unique_id':'stable-'+str(mid),'width':1280,'height':960,'file_size':4}],
        'caption':'https://t.me/example/10'}
    if group:message['media_group_id']=group
    if origin=='user':message['forward_origin']={'type':'user','date':100,'sender_user':{'id':99,'is_bot':False,'first_name':'origin'}}
    elif origin=='hidden_user':message['forward_origin']={'type':'hidden_user','date':100,'sender_user_name':'hidden'}
    elif origin=='channel':message['forward_origin']={'type':'channel','date':100,'chat':{'id':-1000000000010,'type':'channel','title':'source'},'message_id':55}
    return {'update_id':mid+100000,'message':message}


class CodecTests(unittest.TestCase):
    def test_pinned_layout_photo_id_and_original_stem(self):
        for major,reference in ((2,b''),(4,b''),(4,b'file-reference')):
            self.assertEqual(photo_id_from_file_id(file_id(major,reference)),PHOTO)
        self.assertEqual(photo_unique_id(PHOTO),'AgADpsgxGwi2aVU')

    def test_modern_minor61_compact_thumbnail_exact_shape(self):
        # Synthetic metadata ONLY: same body/offset/reference lengths as the
        # privately inspected ten real arrivals; no real identifiers retained.
        reference=b'R'*25
        header=struct.pack('<ii',2 | (1<<25),2)+bytes([25])+reference+b'\0'*2
        prefix=header+struct.pack('<qq',PHOTO,12345)
        suffix=struct.pack('<iii',1,2,ord('y'))
        self.assertEqual(len(prefix+suffix),64)
        for minor in (32,61):
            value=rle(prefix+suffix+bytes([minor,4]))
            self.assertEqual(photo_id_from_file_id(value),PHOTO)
            raw=raw_update();raw['message']['photo'][-1]['file_id']=value
            item=payload_for(Update.de_json(raw,None),{OWNER},BOT)['items'][0]
            self.assertEqual(item['archive_meta']['photo_id'],PHOTO)
            self.assertEqual(photo_unique_id(item['archive_meta']['photo_id']),'AgADpsgxGwi2aVU')
        for suffix2 in (suffix[:-1],suffix+b'\0',struct.pack('<iii',0,2,ord('y')),
                        struct.pack('<iii',1,5,ord('y')),struct.pack('<iii',1,2,999)):
            self.assertIsNone(photo_id_from_file_id(rle(prefix+suffix2+bytes([61,4]))))
        self.assertIsNone(photo_id_from_file_id(rle(prefix+suffix+bytes([31,4]))))

    def test_unsupported_and_malformed_are_explicit_fallback(self):
        for value in ('invalid','',rle(b'\0'*50+b'\x09'),'secret-file-id', 'a'*3000):
            self.assertIsNone(photo_id_from_file_id(value))
        raw=raw_update();raw['message']['photo'][-1]['file_id']='future-unsupported'
        item=payload_for(Update.de_json(raw,None),{OWNER},BOT)['items'][0]
        self.assertEqual(item['naming_codec'],'stable-fallback-v1')
        self.assertTrue(item['archive_meta']['filename'].startswith('bot-photo-'))
        self.assertNotIn('photo_id',item['archive_meta'])


class InboundTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=fixtures.CachedResendTests.asyncSetUp
    asyncTearDown=fixtures.CachedResendTests.asyncTearDown
    execute=fixtures.CachedResendTests.execute
    forbid_source=fixtures.CachedResendTests.forbid_source
    assert_no_source=fixtures.CachedResendTests.assert_no_source
    click=fixtures.CachedResendTests.click
    button=fixtures.CachedResendTests.button

    def transport(self):
        self.engine.bot_id=BOT
        async def download(out):out.write(b'data')
        async def get_file(fid):
            return NS(file_unique_id=self.current_unique,file_size=4,download_to_memory=AsyncMock(side_effect=download))
        self.bot.get_file=AsyncMock(side_effect=get_file)
        self.forbid_source()

    async def queue(self,raw=None):
        raw=raw or raw_update()
        self.current_unique=raw['message']['photo'][-1]['file_unique_id']
        await self.controller.handle(Update.de_json(raw,None))
        return self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]

    async def test_ten_real_shaped_photos_automatic_jobs_duplicate_restart_late(self):
        self.transport()
        for mid in range(73263,73273):await self.queue(raw_update(mid))
        await self.queue(raw_update())
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],10)
        self.bot.send_message.assert_not_awaited()  # no preview/confirmation
        self.store.recover()
        for mid in range(73263,73273):
            self.current_unique='stable-'+str(mid)
            row=await self.execute();self.assertEqual(row['state'],'done',row['error'])
        self.assertEqual(len(list(self.config.root.rglob('*.jpg'))),10)
        self.assertEqual(self.bot.get_file.await_count,10)
        await self.queue(raw_update(73273))
        self.assertEqual((await self.execute())['state'],'done')
        self.assertEqual(self.bot.get_file.await_count,11)
        self.assert_no_source();self.bot.send_photo.assert_not_awaited();self.bot.send_media_group.assert_not_awaited()
        await self.queue(raw_update())
        self.assertIsNone(self.store.claim())

    async def test_all_origins_caption_url_bypass_source_and_preserve_received_naming(self):
        self.transport()
        for n,origin in enumerate(('user','hidden_user','channel',None)):
            await self.queue(raw_update(73263+n,origin=origin))
            task=await self.execute();self.assertEqual(task['state'],'done',task['error'])
            payload=json.loads(task['payload']);item=payload['items'][0]
            path=self.engine.archive.file(payload['snapshot'],item)
            self.assertEqual(path.name,str(73263+n)+' - AgADpsgxGwi2aVU.jpg')
            self.assertEqual(path.relative_to(self.config.root).parts[0],str(OWNER))
            self.assertEqual(path.parent.name,'2026_11')
            self.assertEqual(item['album'],'actual-album')
            manifest=(self.config.root/'.metadata'/('manifest_'+task['id']+'.json')).read_text()
            self.assertNotIn('file_id',manifest)
        self.assert_no_source();self.bot.send_message.assert_not_awaited()

    async def test_private_actor_only(self):
        self.transport()
        variants=[]
        variants.append(raw_update(uid=192114587))
        for modification in ({'chat':{'id':-10001,'type':'supergroup'}},{'sender_chat':{'id':-10001,'type':'channel','title':'anon'}},{'from':{'id':OWNER,'is_bot':True,'first_name':'bot'}}):
            raw=raw_update();raw['message'].update(modification);variants.append(raw)
        for raw in variants:await self.queue(raw)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],0)
        self.bot.get_file.assert_not_awaited();self.bot.send_message.assert_not_awaited();self.assert_no_source()

    async def test_restart_existing_final_file_skips_api_and_cache_resend_still_explicit(self):
        self.transport();await self.queue();row=await self.execute();self.assertEqual(row['state'],'done',row['error'])
        self.store.db.execute('DELETE FROM cache');self.store.set_task(row['id'],state='running')
        self.store.recover();self.bot.get_file.reset_mock()
        self.assertEqual((await self.execute())['state'],'done');self.bot.get_file.assert_not_awaited()
        await self.controller.render_task(OWNER,OWNER,100,self.store.task(row['id'],OWNER,OWNER))
        await self.click(self.button());sent=await self.execute()
        self.assertEqual(sent['state'],'done',sent['error']);self.bot.send_photo.assert_awaited_once();self.assert_no_source()

    async def test_size_identity_limits_and_partial_cleanup(self):
        self.transport()
        for size in (0,CLOUD_DOWNLOAD_LIMIT+1):
            raw=raw_update();raw['message']['photo'][-1]['file_size']=size
            with self.assertRaises(ValueError):payload_for(Update.de_json(raw,None),{OWNER},BOT)
        await self.queue()
        async def short(out):out.write(b'da')
        self.bot.get_file=AsyncMock(return_value=NS(file_unique_id='stable-73263',file_size=4,download_to_memory=AsyncMock(side_effect=short)))
        self.assertEqual((await self.execute())['state'],'failed')
        self.assertFalse(list(self.config.root.rglob('.download-*')));self.assertFalse(list(self.config.root.rglob('*.jpg')))

    async def test_identity_bot_mismatch_and_collision_no_overwrite(self):
        self.transport();await self.queue()
        row=self.store.claim();self.engine.bot_id=BOT+1
        await self.engine.execute(row)
        self.assertEqual(self.store.task(row['id'],OWNER,OWNER)['state'],'failed');self.bot.get_file.assert_not_awaited()
        self.engine.bot_id=BOT;self.store.set_task(row['id'],state='queued',due=0)
        payload=json.loads(row['payload']);dest=self.engine.archive.file(payload['snapshot'],payload['items'][0])
        dest.parent.mkdir(parents=True);dest.write_bytes(b'bad');dest.chmod(0o600)
        self.assertEqual((await self.execute())['state'],'failed');self.assertEqual(dest.read_bytes(),b'bad')

    async def test_download_errors_private_and_retry_delay(self):
        self.transport();await self.queue()
        self.bot.get_file=AsyncMock(side_effect=OSError('https://api.telegram.org/file/botSECRET/file'))
        row=await self.execute();self.assertNotIn('SECRET',row['error'])
        self.store.set_task(row['id'],state='queued',due=0)
        self.bot.get_file=AsyncMock(side_effect=RetryAfter(2))
        self.assertEqual((await self.execute())['state'],'delayed')

    async def test_cancellation_cleans_only_owned_temporary(self):
        self.transport();await self.queue();started=asyncio.Event()
        async def hold(out):out.write(b'd');started.set();await asyncio.Event().wait()
        self.bot.get_file=AsyncMock(return_value=NS(file_unique_id='stable-73263',file_size=4,download_to_memory=AsyncMock(side_effect=hold)))
        worker=asyncio.create_task(self.engine.execute(self.store.claim()));await started.wait();worker.cancel()
        with self.assertRaises(asyncio.CancelledError):await worker
        self.assertFalse(list(self.config.root.rglob('.download-*')));self.assertFalse(list(self.config.root.rglob('*.jpg')))

    async def cancel_acquisition(self, inbound):
        import tempfile
        acquired=threading.Event();release=threading.Event();resources=[]
        real=tempfile.mkstemp
        def slow(*args,**kwargs):
            result=real(*args,**kwargs)
            if kwargs.get('prefix')=='.download-':
                resources.append(result);acquired.set()
                if not release.wait(5):raise AssertionError('test release timed out')
            return result
        if inbound:
            self.transport();await self.queue()
            operation=self.engine.execute(self.store.claim())
        else:
            operation=fixtures.CachedResendTests.original(self,1)
        with patch('tg_forwarder.control.engine.tempfile.mkstemp',side_effect=slow):
            worker=asyncio.create_task(operation)
            try:
                self.assertTrue(await asyncio.to_thread(acquired.wait,3))
                worker.cancel();await asyncio.sleep(.01);worker.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError):await worker
            finally:
                release.set()
                if not worker.done():
                    worker.cancel();await asyncio.gather(worker,return_exceptions=True)
        self.assertEqual(len(resources),1)
        fd,name=resources[0]
        self.assertFalse(Path(name).exists())
        with self.assertRaises(OSError) as failure:os.fstat(fd)
        self.assertEqual(failure.exception.errno,errno.EBADF)
        self.assertFalse(list(self.config.root.rglob('.download-*')))

    async def test_cancel_at_acquisition_closes_fd_and_temp_inbound(self):
        await self.cancel_acquisition(True)

    async def test_cancel_at_acquisition_closes_fd_and_temp_mtproto(self):
        await self.cancel_acquisition(False)

    async def test_recovery_exact_done_rows_idempotent_no_history_mutation(self):
        self.transport();self.current_unique="stable-73263";raw=raw_update();self.store.ingest(raw)
        self.store.db.execute("UPDATE updates SET state='done'")
        text=self.store.db.execute('SELECT payload FROM updates').fetchone()[0]
        plan={'version':1,'owner_uid':OWNER,'bot_id':BOT,'db':str(self.config.db),
              'updates':[{'update_id':raw['update_id'],'payload_sha256':hashlib.sha256(text.encode()).hexdigest()}]}
        offset=self.store.get_meta('offset')
        self.assertEqual(recover(self.config,plan)['validated_updates'],1)
        self.assertIsNone(self.store.claim())
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':str(BOT)+':offline'}):
            first=recover(self.config,plan,apply=True,quiescent=True)
            self.assertEqual(first,recover(self.config,plan,apply=True,quiescent=True))
        self.assertEqual(self.store.get_meta('offset'),offset)
        self.assertEqual(tuple(self.store.db.execute('SELECT payload,state FROM updates').fetchone()),(text,'done'))
        self.assertEqual((await self.execute())['state'],'done')
        plan['updates'][0]['payload_sha256']='0'*64
        with self.assertRaises(ValueError):recover(self.config,plan)

    async def test_recovery_lock_owner_token_quiescence_reject(self):
        self.transport();self.current_unique="stable-73263";raw=raw_update();self.store.ingest(raw);self.store.db.execute("UPDATE updates SET state='done'")
        text=self.store.db.execute('SELECT payload FROM updates').fetchone()[0]
        plan={'version':1,'owner_uid':OWNER,'bot_id':BOT,'db':str(self.config.db),
              'updates':[{'update_id':raw['update_id'],'payload_sha256':hashlib.sha256(text.encode()).hexdigest()}]}
        with self.assertRaises(ValueError):recover(self.config,plan,apply=True)
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':'123:offline'}):
            with self.assertRaises(ValueError):recover(self.config,plan,apply=True,quiescent=True)
        with open(str(self.config.db)+'.lock','a') as lock, patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':str(BOT)+':offline'}):
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaises(ValueError):recover(self.config,plan,apply=True,quiescent=True)
        self.assertIsNone(self.store.claim())

    async def test_unknown_size_and_remote_identity_size_rejection(self):
        self.transport()
        raw=raw_update();raw['message']['photo'][-1].pop('file_size')
        await self.queue(raw)
        async def bytes_out(out):out.write(b'data')
        self.bot.get_file=AsyncMock(return_value=NS(file_unique_id='stable-73263',file_size=None,download_to_memory=AsyncMock(side_effect=bytes_out)))
        self.assertEqual((await self.execute())['state'],'done')
        await self.queue(raw_update(73264))
        self.bot.get_file=AsyncMock(return_value=NS(file_unique_id='wrong',file_size=4,download_to_memory=AsyncMock(side_effect=bytes_out)))
        self.assertEqual((await self.execute())['state'],'failed')
        await self.queue(raw_update(73265))
        remote=NS(file_unique_id='stable-73265',file_size=CLOUD_DOWNLOAD_LIMIT+1,download_to_memory=AsyncMock())
        self.bot.get_file=AsyncMock(return_value=remote)
        self.assertEqual((await self.execute())['state'],'failed');remote.download_to_memory.assert_not_awaited()

    async def test_actual_byte_guard_disk_full_and_symlink_no_clobber(self):
        self.transport();await self.queue()
        self.engine.config=replace(self.config,max_bytes=4)
        async def large(out):out.write(b'too large')
        remote=NS(file_unique_id='stable-73263',file_size=4,download_to_memory=AsyncMock(side_effect=large))
        self.bot.get_file=AsyncMock(return_value=remote)
        self.assertEqual((await self.execute())['state'],'failed')
        self.assertFalse(list(self.config.root.rglob('.download-*')))
        await self.queue(raw_update(73264))
        with patch.object(self.engine.archive,'space',side_effect=OSError('disk full')):
            self.assertEqual((await self.execute())['state'],'failed')
        await self.queue(raw_update(73265))
        row=self.store.db.execute('SELECT * FROM tasks ORDER BY rowid DESC LIMIT 1').fetchone()
        payload=json.loads(row['payload']);path=self.engine.archive.file(payload['snapshot'],payload['items'][0])
        path.parent.mkdir(parents=True,exist_ok=True)
        target=self.root/'external';target.write_bytes(b'keep');path.symlink_to(target)
        self.assertEqual((await self.execute())['state'],'failed');self.assertEqual(target.read_bytes(),b'keep')

    async def test_restart_actually_reopens_state_and_recovery_is_atomic(self):
        self.transport();raw=raw_update();await self.queue(raw)
        row=self.store.claim();self.store.close()
        self.store=Store(self.config.db);self.engine.store=self.store;self.gate.store=self.store;self.controller.store=self.store
        self.store.recover();self.assertEqual((await self.execute())['state'],'done')
        raw1=raw_update(73264);raw2=raw_update(73265)
        for raw in (raw1,raw2):self.store.ingest(raw)
        self.store.db.execute("UPDATE updates SET state='done'")
        entries=[{'update_id':r['id'],'payload_sha256':hashlib.sha256(r['payload'].encode()).hexdigest()} for r in self.store.db.execute('SELECT * FROM updates')]
        entries[-1]['payload_sha256']='bad'
        plan={'version':1,'owner_uid':OWNER,'bot_id':BOT,'db':str(self.config.db),'updates':entries}
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':str(BOT)+':offline'}):
            with self.assertRaises(ValueError):recover(self.config,plan,apply=True,quiescent=True)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],1)


class OwnerAndCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_cache_no_filesystem_or_index_while_rate_receipts_work(self):
        redis=Redis();bridge=SharedMedia(redis,'/unreadable/foreign-private',account_rate_key='test',
            cache_read_enabled=False,persistent_cache_enabled=False,min_free_bytes=0,rate_interval=.001)
        with patch.object(bridge,'_root',side_effect=AssertionError('FS touched')), patch.object(redis,'get',AsyncMock(side_effect=AssertionError('index touched'))):
            self.assertIsNone(await bridge.cached(['1',1,'x']))
            self.assertIsNone(await bridge.materialize(['1',1,'x'],Path('/unused')))
            self.assertEqual(await bridge.publish(['1',1,'x'],'/spool'),Path('/spool'))
        await bridge.wait()
        sent=AsyncMock(return_value=NS(sent_message_id=5))
        await bridge.deliver('receipt',sent);await bridge.deliver('receipt',sent)
        sent.assert_awaited_once()

    async def test_explicit_nfs_uid_only_and_permissions_symlink_still_rejected(self):
        import tempfile
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);path=root/'photo';path.write_bytes(b'data');path.chmod(0o600)
            owner_uid=[1024]
            original=os.lstat
            def mapped(p):
                info=original(p)
                values=list(info);values[4]=owner_uid[0] if p==path else 1026
                return os.stat_result(values)
            with patch('tg_forwarder.canonical_media.os.getuid',return_value=1000),patch.object(Path,'lstat',mapped),patch.object(Path,'stat',lambda p,*a,**kw:mapped(p)):
                with patch.dict(os.environ,{'MEDIA_ARCHIVE_FILE_OWNER_UIDS':''}):
                    with self.assertRaises(ValueError):secure_path(root,'photo',existing=True)
                with patch.dict(os.environ,{'MEDIA_ARCHIVE_FILE_OWNER_UIDS':'1024'}):
                    self.assertEqual(secure_path(root,'photo',existing=True),path)
                    path.chmod(0o622)
                    with self.assertRaises(ValueError):secure_path(root,'photo',existing=True)
            path.chmod(0o600)
            with patch('tg_forwarder.canonical_media.os.getuid',return_value=1000),patch.object(Path,'lstat',mapped),patch.object(Path,'stat',lambda p,*a,**kw:mapped(p)):
                owner_uid[0]=0
                with patch.dict(os.environ,{'MEDIA_ARCHIVE_FILE_OWNER_UIDS':'1024'}):
                    with self.assertRaises(ValueError):secure_path(root,'photo',existing=True)
                with patch.dict(os.environ,{'MEDIA_ARCHIVE_FILE_OWNER_UIDS':'0,1024'}):
                    self.assertEqual(trusted_file_owner_uids(),frozenset({0,1024}))
                    self.assertEqual(secure_path(root,'photo',existing=True),path)
                    owner_uid[0]=9999
                    with self.assertRaises(ValueError):secure_path(root,'photo',existing=True)
            link=root/'link';link.symlink_to(path)
            with patch.dict(os.environ,{'MEDIA_ARCHIVE_FILE_OWNER_UIDS':'1024'}):
                with self.assertRaises(ValueError):secure_path(root,'link',existing=True)
        for raw in ('-1','any','1024,','4294967295'):
            with patch.dict(os.environ,{'MEDIA_ARCHIVE_FILE_OWNER_UIDS':raw}):
                with self.assertRaises(ValueError):trusted_file_owner_uids()
