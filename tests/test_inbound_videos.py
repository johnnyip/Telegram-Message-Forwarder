import copy
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock,patch
from telegram import Update
import test_album_actions as albums
import test_inbound_photos as inbound
from tg_forwarder.control.inbound import payload_for,video_binding,CLOUD_DOWNLOAD_LIMIT
from tg_forwarder.control.recover_inbound import recover
from tg_forwarder.control.store import encode
from tg_forwarder.control.delivery import BotDelivery


class VideoInboundTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=albums.AlbumActionTests.asyncSetUp
    asyncTearDown=albums.AlbumActionTests.asyncTearDown
    forbid_source=albums.AlbumActionTests.forbid_source
    assert_no_source=albums.AlbumActionTests.assert_no_source
    execute=albums.AlbumActionTests.execute
    add=albums.AlbumActionTests.add
    mature=albums.AlbumActionTests.mature
    drain=albums.AlbumActionTests.drain
    video=albums.AlbumActionTests.video

    def transport(self):
        async def get_file(fid):
            async def output(out):out.write(b'data')
            return NS(file_id=fid,file_unique_id=self.current_unique,file_size=4,download_to_memory=AsyncMock(side_effect=output))
        self.bot.get_file=AsyncMock(side_effect=get_file)
        self.engine.delivery=BotDelivery(self.bot);self.engine.bot_id=inbound.BOT
        self.forbid_source()

    async def queue(self,raw):
        media=raw['message'].get('video') or raw['message'].get('photo',[])[-1]
        self.current_unique=media['file_unique_id']
        await self.controller.handle(Update.de_json(raw,None))

    async def test_actual_controller_ten_photos_and_mixed_seven_two_complete_cards(self):
        for mid in range(91000,91010):await self.add(mid,'photos')
        for mid in range(91100,91102):await self.add(mid,'mixed')
        for mid in range(91102,91107):await self.queue(self.video(mid,size=4))
        await self.drain(17)
        self.assertEqual(self.bot.send_message.await_count,2)
        rows=self.store.db.execute('SELECT * FROM inbound_notices').fetchall()
        self.assertEqual(len(rows),2)
        for row in rows:
            _,tasks,sup,revision=self.engine.album_snapshot(inbound.OWNER,inbound.OWNER,row['key'])
            self.assertFalse(sup);self.assertEqual(len((await self.controller.album_buttons(row,tasks,sup,revision,row['reply']))[0]),2)
            self.assertTrue(all(t['state']=='done' for t in tasks))
        self.assertEqual(self.bot.get_file.await_count,17);self.assert_no_source()
        files=list(self.config.root.rglob('*.mp4'));self.assertEqual(len(files),5)
        self.assertTrue(all(p.parent.name=='Johnny 叔叔' and ':' not in p.name for p in files))
        self.assertFalse(self.store.db.execute("SELECT 1 FROM meta WHERE key LIKE 'flat_photo_alias:%' AND value LIKE '%bot-inbound-video:%'").fetchone())

    async def test_mixed_cached_album_one_group_correct_order_no_getfile(self):
        for mid in range(91200,91202):await self.add(mid,'mixed')
        for mid in range(91202,91207):await self.queue(self.video(mid,size=4))
        await self.drain(7);before=self.bot.get_file.await_count
        row=self.store.db.execute('SELECT * FROM inbound_notices').fetchone()
        _,_,_,revision=self.engine.album_snapshot(inbound.OWNER,inbound.OWNER,row['key'])
        self.bot.send_media_group=AsyncMock(return_value=[NS(message_id=200000+i) for i in range(7)])
        await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],{'notice':row['key'],'revision':revision,'do':'cached_photos'})
        result=await self.execute();self.assertEqual(result['state'],'done',result['error'])
        self.assertEqual(self.bot.get_file.await_count,before);self.assert_no_source()
        media=self.bot.send_media_group.call_args.kwargs['media']
        self.assertEqual([m.type for m in media],['photo','photo','video','video','video','video','video'])
        self.bot.send_media_group.assert_awaited_once()
        payload=json.loads(result['payload']);self.assertEqual([i['id'] for i in payload['items']],list(range(91200,91207)))

    async def test_video_wrong_binding_bot_owner_header_and_oversize_rejected_before_fetch(self):
        raw=self.video(91300,size=4);payload=payload_for(Update.de_json(raw,None),self.config.allowed,inbound.BOT)
        task={'owner':inbound.OWNER,'chat':inbound.OWNER,'id':'not-needed'}
        for field,value in (('file_id','invalid-video'),('bot_id',inbound.BOT+1),('chat',192114587),('size',5),('kind','photo')):
            changed=copy.deepcopy(payload);changed['items'][0][field]=value
            with self.assertRaises((ValueError,PermissionError,KeyError)):await self.engine.download(task,changed,changed['items'][0],0)
        self.bot.get_file.assert_not_awaited()
        raw=self.video(91301,size=CLOUD_DOWNLOAD_LIMIT+1);await self.queue(raw)
        self.bot.get_file.assert_not_awaited();self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],0)
        self.mature();await self.controller.inbound_notice_tick()
        self.assertIn('未下載影片 1',self.bot.send_message.call_args.kwargs['text'])

    async def test_video_remote_unique_size_type_mismatches_fail_no_published_file(self):
        for number,kind in enumerate(('unique','size','type')):
            raw=self.video(91400+number,size=4);await self.queue(raw)
            media=raw['message']['video']
            self.bot.get_file=AsyncMock(return_value=NS(file_id=media['file_id'] if kind!='type' else 'invalid',file_unique_id=media['file_unique_id'] if kind!='unique' else 'wrong',file_size=4 if kind!='size' else 5,download_to_memory=AsyncMock()))
            result=await self.execute();self.assertEqual(result['state'],'failed')
        self.assertFalse(list(self.config.root.rglob('*.mp4')))

    async def test_mixed_cached_album_same_size_photo_corruption_blocks_before_transport_claim(self):
        for mid in range(91600,91602):await self.add(mid,'mixed')
        for mid in range(91602,91607):await self.queue(self.video(mid,size=4))
        await self.drain(7)
        row=self.store.db.execute('SELECT * FROM inbound_notices').fetchone()
        _,_,_,revision=self.engine.album_snapshot(inbound.OWNER,inbound.OWNER,row['key'])
        self.bot.send_media_group=AsyncMock();before=self.bot.get_file.await_count
        path=Path(self.store.db.execute("SELECT path FROM cache WHERE path LIKE '%.jpg' LIMIT 1").fetchone()[0]);path.write_bytes(b'evil')
        with self.assertRaises(ValueError):await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],{'notice':row['key'],'revision':revision,'do':'cached_photos'})
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],7)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM operations').fetchone()[0],0)
        self.bot.send_media_group.assert_not_awaited();self.assertEqual(self.bot.get_file.await_count,before)

    async def test_video_cached_corruption_or_writable_file_never_fetches_source(self):
        await self.queue(self.video(91450,size=4));task=await self.execute()
        self.assertEqual(task['state'],'done',task['error']);item=json.loads(task['payload'])['items'][0]
        path=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0]);before=self.bot.get_file.await_count
        path.write_bytes(b'evil')
        with self.assertRaises(ValueError):await self.engine.cached_file_async(item)
        with self.assertRaises(ValueError):await self.engine.download(task,json.loads(task['payload']),item,0)
        path.chmod(0o666)
        with self.assertRaises(ValueError):await self.engine.cached_file_async(item)
        self.assertEqual(self.bot.get_file.await_count,before);self.assertEqual(path.read_bytes(),b'evil')

    async def test_retained_unqueued_five_video_placeholders_survive_restart_no_partial_send(self):
        from tg_forwarder.control.controller import Controller
        for mid in range(91700,91702):await self.add(mid,'mixed')
        await self.drain(2)
        for mid in range(91702,91707):
            raw=self.video(mid,size=4);self.store.ingest(raw)
            self.store.db.execute("UPDATE updates SET state='done' WHERE id=?",(raw['update_id'],))
        offset=self.store.get_meta('offset')
        self.controller.refresh_inbound_templates()
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        self.controller.refresh_inbound_templates()
        row=self.store.db.execute('SELECT * FROM inbound_notices').fetchone()
        _,tasks,pending,revision=self.engine.album_snapshot(inbound.OWNER,inbound.OWNER,row['key'])
        self.assertEqual(len(tasks),2);self.assertEqual(len(pending),5)
        self.assertIn('2/7',await self.controller.album_text(row,tasks,pending))
        self.assertEqual(len((await self.controller.album_buttons(row,tasks,pending,revision,row['reply']))[0]),1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)
        self.assertEqual(self.store.get_meta('offset'),offset)
        self.assertEqual(self.bot.send_message.await_count,1)

    async def test_exact_five_retained_videos_attach_existing_anchor_idempotently_no_offset_change(self):
        for mid in range(91500,91502):await self.add(mid,'mixed')
        await self.drain(2);anchor=self.store.db.execute('SELECT reply FROM inbound_notices').fetchone()[0]
        plan={'version':1,'owner_uid':inbound.OWNER,'bot_id':inbound.BOT,'db':str(self.config.db),'updates':[]}
        for mid in range(91502,91507):
            raw=self.video(mid,size=4);self.store.ingest(raw)
            self.store.db.execute("UPDATE updates SET state='done' WHERE id=?",(raw['update_id'],))
            text=self.store.db.execute('SELECT payload FROM updates WHERE id=?',(raw['update_id'],)).fetchone()[0]
            plan['updates'].append({'update_id':raw['update_id'],'payload_sha256':hashlib.sha256(text.encode()).hexdigest()})
        key=self.store.db.execute('SELECT key FROM inbound_notices').fetchone()[0]
        self.controller.refresh_inbound_templates();self.mature();await self.controller.inbound_notice_tick()
        self.assertIn('2/7',self.bot.edit_message_text.call_args.kwargs['text'])
        self.assertIn('影片尚未下載／待補做 5',self.bot.edit_message_text.call_args.kwargs['text'])
        row=self.store.db.execute('SELECT * FROM inbound_notices').fetchone()
        _,tasks,pending,revision=self.engine.album_snapshot(inbound.OWNER,inbound.OWNER,row['key'])
        self.assertEqual(len(pending),5)
        self.assertTrue(all(p['state']=='awaiting-task' and len(p['payload_sha256'])==64 for p in pending))
        self.assertEqual(len((await self.controller.album_buttons(row,tasks,pending,revision,row['reply']))[0]),1)
        with self.assertRaises(ValueError):self.engine.cached_album_payload(inbound.OWNER,inbound.OWNER,row['key'],revision)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)
        offset=self.store.get_meta('offset');history=[tuple(r) for r in self.store.db.execute('SELECT * FROM updates')]
        self.assertEqual(recover(self.config,plan)['validated_updates'],5)
        bad=copy.deepcopy(plan);bad['updates'][-1]['payload_sha256']='0'*64
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':str(inbound.BOT)+':offline'}):
            with self.assertRaises(ValueError):recover(self.config,bad,apply=True,quiescent=True)
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)
            result=recover(self.config,plan,apply=True,quiescent=True)
            self.assertEqual(result,recover(self.config,plan,apply=True,quiescent=True))
        row=self.store.db.execute('SELECT * FROM inbound_notices').fetchone()
        self.assertEqual(row['reply'],anchor);self.assertEqual(len(json.loads(row['payload'])),7)
        self.assertEqual(json.loads(self.store.get_meta('inbound-unsupported:'+key)),[])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM tasks WHERE state='queued'").fetchone()[0],5)
        self.assertEqual(self.store.get_meta('offset'),offset);self.assertEqual([tuple(r) for r in self.store.db.execute('SELECT * FROM updates')],history)
        await self.drain(5);self.assertEqual(self.bot.send_message.await_count,1)
        self.assertIn('7/7',self.bot.edit_message_text.call_args.kwargs['text'])
        self.store.meta('inbound-ui-template:'+str(inbound.BOT)+':'+str(inbound.OWNER),'')
        self.controller.refresh_inbound_templates()
        self.assertEqual(json.loads(self.store.get_meta('inbound-unsupported:'+key)),[])
        self.assertEqual(len(json.loads(self.store.db.execute('SELECT payload FROM inbound_notices').fetchone()[0])),7)
