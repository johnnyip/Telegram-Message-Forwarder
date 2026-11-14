import asyncio
import json
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock
import test_named_albums as named
import test_inbound_photos as inbound
from tg_forwarder.control.controller import Controller


class AlbumActionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=named.NamedAlbumTests.asyncSetUp
    asyncTearDown=named.NamedAlbumTests.asyncTearDown
    execute=named.NamedAlbumTests.execute
    forbid_source=named.NamedAlbumTests.forbid_source
    assert_no_source=named.NamedAlbumTests.assert_no_source
    transport=named.NamedAlbumTests.transport
    async def queue(self,raw):
        from telegram import Update
        if raw['message'].get('photo'):self.current_unique=raw['message']['photo'][-1]['file_unique_id']
        await self.controller.handle(Update.de_json(raw,None))
    add=named.NamedAlbumTests.add
    drain=named.NamedAlbumTests.drain
    mature=named.NamedAlbumTests.mature

    def video(self,mid,album='mixed',size=20000001):
        import base64,struct
        wire=struct.pack('<iiqq',4,5,1000+mid,42)+bytes([61,4]);packed=bytearray();i=0
        while i<len(wire):
            if wire[i]:packed.append(wire[i]);i+=1
            else:
                j=i
                while j<len(wire) and not wire[j] and j-i<255:j+=1
                packed.extend((0,j-i));i=j
        fid=base64.urlsafe_b64encode(packed).decode().rstrip('=')
        raw=inbound.raw_update(mid,group=album);raw['message'].pop('photo')
        raw['message']['chat'].update(first_name='Johnny',last_name='叔叔');raw['message']['from'].update(first_name='Johnny',last_name='叔叔')
        raw['message']['video']={'file_id':fid,'file_unique_id':'video-'+str(mid),'width':704,'height':1280,'duration':18,'file_size':size,'file_name':'movie:'+str(mid)+'.mp4','mime_type':'video/mp4'}
        return raw

    async def full(self,n=10,group='full',start=90000):
        for mid in range(start,start+n):await self.add(mid,group)
        await self.drain(n)
        row=self.store.db.execute("SELECT * FROM inbound_notices WHERE key NOT LIKE 'cached-operation:%' ORDER BY rowid DESC LIMIT 1").fetchone()
        _,_,_,revision=self.engine.album_snapshot(inbound.OWNER,inbound.OWNER,row['key'])
        return row,revision

    async def test_two_actual_groups_ten_photo_and_two_photo_five_video_two_truthful_cards(self):
        for mid in range(90000,90010):await self.add(mid,'full')
        for mid in range(90100,90102):await self.add(mid,'mixed')
        for mid in range(90102,90107):await self.queue(self.video(mid))
        self.mature();await self.controller.inbound_notice_tick();await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,2)
        await self.drain(12);self.assertEqual(self.bot.send_message.await_count,2)
        rows=self.store.db.execute('SELECT * FROM inbound_notices').fetchall()
        for row in rows:
            _,tasks,sup,rev=self.engine.album_snapshot(inbound.OWNER,inbound.OWNER,row['key'])
            buttons=await self.controller.album_buttons(row,tasks,sup,rev,row['reply'])
            text=await self.controller.album_text(row,tasks,sup)
            if sup:
                self.assertIn('2/7',text);self.assertIn('未下載影片 5',text);self.assertNotIn('下載完成',text)
                self.assertEqual(len(buttons[0]),1)
                with self.assertRaises(ValueError):self.engine.cached_album_payload(inbound.OWNER,inbound.OWNER,row['key'],rev)
            else:self.assertEqual(len(buttons[0]),2);self.assertIn('10/10',text)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],12)
        self.assertFalse(any('來源已隱藏' in c.kwargs.get('text','') for c in self.bot.send_message.await_args_list))

    async def test_click_whole_album_one_group_upload_no_source_fetch_and_separate_card(self):
        row,rev=await self.full()
        before=self.bot.get_file.await_count;self.bot.send_media_group=AsyncMock(return_value=[NS(message_id=100000+i) for i in range(10)])
        data={'notice':row['key'],'revision':rev,'do':'cached_photos'}
        await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],data)
        self.bot.send_media_group.assert_not_awaited()
        self.mature();await self.controller.inbound_notice_tick()
        op=self.store.db.execute("SELECT * FROM inbound_notices WHERE key LIKE 'cached-operation:%'").fetchone()
        self.assertNotEqual(op['reply'],row['reply']);self.assertGreater(op['reply'],0)
        result=await self.execute();self.assertEqual(result['state'],'done',result['error'])
        self.assertEqual(self.bot.get_file.await_count,before);self.assert_no_source()
        self.bot.send_media_group.assert_awaited_once();self.assertEqual(len(self.bot.send_media_group.call_args.kwargs['media']),10)
        self.mature();await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,2)
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],data)
        self.assertIsNone(self.store.claim());self.assertEqual(self.bot.send_media_group.await_count,1)
        self.assertEqual(self.store.db.execute('SELECT reply FROM inbound_notices WHERE key=?',(row['key'],)).fetchone()[0],row['reply'])

    async def test_late_video_invalidates_capability_and_blocks_partial_photo_upload(self):
        row,rev=await self.full(2,'mixed',90200)
        await self.queue(self.video(90202))
        with self.assertRaises(ValueError):await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],{'notice':row['key'],'revision':rev,'do':'cached_photos'})
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)

    async def test_details_one_anchor_and_wrong_owner_bot_reply_revision_rejected(self):
        row,rev=await self.full(2)
        data={'notice':row['key'],'revision':rev,'do':'details'}
        count=self.bot.send_message.await_count
        await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],data)
        self.assertEqual(self.bot.send_message.await_count,count);self.assertIn('90000',self.bot.edit_message_text.call_args.kwargs['text'])
        for owner,chat,reply in ((192114587,inbound.OWNER,row['reply']),(inbound.OWNER,inbound.OWNER,row['reply']+1)):
            with self.assertRaises(PermissionError):await self.controller.album_callback(owner,chat,reply,data)
        self.engine.bot_id+=1
        with self.assertRaises(PermissionError):await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],data)

    async def test_missing_cached_member_prevents_whole_album_upload_no_download(self):
        row,rev=await self.full(2);before=self.bot.get_file.await_count
        from pathlib import Path
        path=Path(self.store.db.execute('SELECT path FROM cache LIMIT 1').fetchone()[0]);path.unlink()
        with self.assertRaises(ValueError):await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],{'notice':row['key'],'revision':rev,'do':'cached_photos'})
        self.assertEqual(self.bot.get_file.await_count,before);self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)

    async def test_cached_operation_ambiguous_card_send_restart_does_not_duplicate(self):
        row,rev=await self.full(2)
        await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],{'notice':row['key'],'revision':rev,'do':'cached_photos'})
        self.bot.send_message.side_effect=ConnectionError('unknown')
        self.mature();await self.controller.inbound_notice_tick();count=self.bot.send_message.await_count
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)
        await self.controller.inbound_notice_tick();self.assertEqual(self.bot.send_message.await_count,count)
        op=self.store.db.execute("SELECT state FROM inbound_notices WHERE key LIKE 'cached-operation:%'").fetchone()
        self.assertEqual(op['state'],'ambiguous')

    async def test_incomplete_group_upload_receipt_is_ambiguous_no_blind_resend(self):
        row,rev=await self.full(2)
        self.bot.send_media_group=AsyncMock(return_value=[NS(message_id=199999)])
        data={'notice':row['key'],'revision':rev,'do':'cached_photos'}
        await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],data)
        result=await self.execute();self.assertEqual(result['state'],'ambiguous')
        await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],data)
        self.assertIsNone(self.store.claim());self.bot.send_media_group.assert_awaited_once()

    async def test_cancelled_whole_album_job_does_not_upload(self):
        row,rev=await self.full(2)
        self.bot.send_media_group=AsyncMock()
        await self.controller.album_callback(inbound.OWNER,inbound.OWNER,row['reply'],{'notice':row['key'],'revision':rev,'do':'cached_photos'})
        cached=self.store.db.execute("SELECT id FROM tasks WHERE state='queued'").fetchone()
        self.controller.change_task(inbound.OWNER,inbound.OWNER,cached['id'],'cancel')
        self.assertIsNone(self.store.claim());self.bot.send_media_group.assert_not_awaited()

    async def test_startup_refresh_known_anchor_backfills_retained_video_without_replay(self):
        row,rev=await self.full(2,'mixed',90300)
        for mid in range(90302,90307):
            raw=self.video(mid);self.store.ingest(raw)
            self.store.db.execute("UPDATE updates SET state='done' WHERE id=?",(raw['update_id'],))
        offset=self.store.get_meta('offset');count=self.bot.send_message.await_count
        self.controller.refresh_inbound_templates();self.mature();await self.controller.inbound_notice_tick()
        self.assertEqual(self.bot.send_message.await_count,count);self.assertEqual(self.store.get_meta('offset'),offset)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],2)
        self.assertIn('2/7',self.bot.edit_message_text.call_args.kwargs['text'])
        generation=self.store.db.execute('SELECT generation FROM inbound_notices').fetchone()[0]
        self.controller.refresh_inbound_templates()
        self.assertEqual(self.store.db.execute('SELECT generation FROM inbound_notices').fetchone()[0],generation)

    async def test_genuine_hidden_text_request_still_warns(self):
        raw=inbound.raw_update(90400);raw['message'].pop('photo');raw['message'].pop('media_group_id');raw['message']['text']='hidden forwarded text'
        await self.queue(raw)
        self.assertIn('來源已隱藏',self.bot.send_message.call_args.kwargs['text'])
