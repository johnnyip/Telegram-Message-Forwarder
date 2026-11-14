"""Offline authoritative resolver -> native download/cache/URL resubmit proofs."""
import json
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch,AsyncMock
import test_named_albums as named
import test_inbound_photos as inbound
import test_media_workbench as source
from tg_forwarder.control.source import Resolver,scopes,Unavailable
from tg_forwarder.control.input import SourceRef
from tg_forwarder.control.store import digest


class MainArchiveTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=named.NamedAlbumTests.asyncSetUp
    asyncTearDown=named.NamedAlbumTests.asyncTearDown
    transport=named.NamedAlbumTests.transport
    forbid_source=named.NamedAlbumTests.forbid_source

    def source(self):
        client=source.FakeClient();client.channel.title='反差婊投稿曝光';client.group.title='评论区'
        client.replies=client.replies[:43]
        self.client=client;self.engine.client=client
        self.controller.resolver=Resolver(client,gate=self.engine.gate,named_folders=True)
        self.store.meta('bot_principal',str(inbound.BOT))
        return client

    async def resolve(self,comment=None):
        with patch('tg_forwarder.control.source.utils.get_peer_id',side_effect=lambda e:e.key):
            return await self.controller.resolver.resolve(SourceRef('tgfanchabiao',4833,comment))

    async def download(self,snap):
        payload={'snapshot':snap,'items':scopes(snap)['all'],'action':'download','target':None}
        task=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,payload)
        if task['state']=='queued':await self.engine.execute(self.store.claim())
        result=self.store.task(task['id'],inbound.OWNER,inbound.OWNER)
        self.assertEqual(result['state'],'done',result['error'])
        return result

    async def submit(self,snap):
        q=self.store.query(inbound.OWNER,inbound.OWNER,78001,snap)
        await self.controller.callback(inbound.OWNER,inbound.OWNER,78001,{'query':q,'scope':'all','do':'download','confirmed':True})

    async def test_native53_all_same_main_root_no_source_identity_changes_and_repeat_delete(self):
        self.source();snap=await self.resolve();values=scopes(snap)['all']
        self.assertEqual(len(values),53)
        self.assertEqual([sum(i['chat']==chat for i in values) for chat in (source.CHANNEL,source.GROUP)],[10,43])
        self.assertTrue(all(i['archive_meta']['folder_chat']==source.CHANNEL for i in values))
        self.assertTrue(all(i['archive_meta']['title']=='评论区' for i in snap['discussion']))
        task=await self.download(snap);original=dict(task)
        paths=[Path(r[0]) for r in self.store.db.execute('SELECT path FROM cache')]
        self.assertEqual(len(paths),53);self.assertTrue(all(p.parent.parent==self.config.root/'反差婊投稿曝光' and p.read_bytes()==b'data' for p in paths))
        self.assertFalse((self.config.root/'评论区').exists())
        before=sum(m.download_media.await_count for m in self.client.main+self.client.replies)
        await self.submit(await self.resolve());self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],1)
        self.assertEqual(sum(m.download_media.await_count for m in self.client.main+self.client.replies),before)
        # Remove the entire main root, not merely stale workbench leaf files.
        import shutil
        shutil.rmtree(self.config.root/'反差婊投稿曝光')
        await self.submit(await self.resolve());head=self.store.get_meta('missing-repair-head:'+task['id'])
        pending=self.store.claim();self.assertEqual(pending['id'],head);await self.engine.execute(pending)
        done=self.store.task(head,inbound.OWNER,inbound.OWNER);self.assertEqual(done['state'],'done',done['error'])
        self.assertEqual(json.loads(self.store.get_meta('task-last-run:'+head))['fresh'],53)
        self.assertTrue(all(Path(r[0]).parent.parent==self.config.root/'反差婊投稿曝光' and Path(r[0]).exists() for r in self.store.db.execute('SELECT path FROM cache')))
        self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER),original)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM operations').fetchone()[0],0)
        await self.controller.render_locations(inbound.OWNER,inbound.OWNER,78001,original,{'revision':digest(original['payload']),'bot':inbound.BOT})
        text=self.bot.edit_message_text.call_args.kwargs['text'];self.assertIn('反差婊投稿曝光',text);self.assertNotIn('评论区',text)
        self.bot.get_file.assert_not_awaited()

    async def test_comment_url_retains_main_root_and_discussion_source(self):
        self.source();snap=await self.resolve(self.client.replies[0].id)
        self.assertEqual(snap['selected'][0]['chat'],source.GROUP)
        self.assertEqual(snap['selected'][0]['archive_meta']['folder_chat'],source.CHANNEL)
        self.assertEqual(self.engine.archive.file(snap,snap['selected'][0]).parent.parent.name,'反差婊投稿曝光')

    async def test_duplicate_message_ids_original_names_deterministic_distinct_nooverwrite(self):
        self.source()
        # Same numeric ID and original document name in different source chats.
        # Distinct source identities remain distinct cache keys and filenames.
        self.client.main=[source.message(source.CHANNEL,4833,kind='video')]
        self.client.main[0].document=NS(id=111,attributes=[NS(file_name='same.mp4')])
        self.client.replies=[source.message(source.GROUP,4833,kind='video')]
        self.client.replies[0].document=NS(id=222,attributes=[NS(file_name='same.mp4')])
        snap=await self.resolve();items=scopes(snap)['all'];self.assertEqual(len(items),2)
        names=[self.engine.archive.file(snap,i) for i in items]
        self.assertNotEqual(names[0],names[1]);self.assertTrue(all('same' in p.name for p in names))
        again=await self.resolve();self.assertEqual(names,[self.engine.archive.file(again,i) for i in scopes(again)['all']])
        # Occupied primary is never overwritten; native secondary no-clobber
        # disambiguation keeps it, and fails closed if both names are occupied.
        names[0].parent.mkdir(parents=True);names[0].write_bytes(b'evil');names[0].chmod(0o600)
        await self.download(snap);self.assertEqual(names[0].read_bytes(),b'evil')
        paths=[Path(r[0]) for r in self.store.db.execute('SELECT path FROM cache')]
        self.assertEqual(len(set(paths)),2);self.assertTrue(all(p.read_bytes()==b'data' for p in paths))

    async def test_standalone_group_root_uses_own_confirmed_title(self):
        self.source();client=self.client
        seed=client.replies[0];seed.grouped_id=None
        client.get_messages=AsyncMock(return_value=seed)
        with patch('tg_forwarder.control.source.utils.get_peer_id',side_effect=lambda e:e.key):
            snap=await self.controller.resolver.resolve(SourceRef(source.GROUP,seed.id))
        self.assertEqual(snap['main'][0]['archive_meta']['folder_chat'],source.GROUP)
        self.assertEqual(self.engine.archive.file(snap,snap['main'][0]).parent.parent.name,'评论区')

    async def test_old_completed_split_cache_left_alone_no_download_or_forward(self):
        self.source();snap=await self.resolve()
        import copy
        old=copy.deepcopy(snap)
        for i in old['main']+old['discussion']:
            meta=i['archive_meta']
            for k in ('folder_chat','folder_title','folder_topic_id','folder_topic_title','folder_collision','folder_month','folder_batch'):meta.pop(k,None)
            path=self.engine.archive.file(old,i);path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'data');path.chmod(0o600)
            key=digest([i['chat'],i['id'],i.get('media_id','')])
            self.store.db.execute('INSERT INTO cache VALUES(?,?,?)',(key,str(path),4))
        payload={'snapshot':old,'items':scopes(old)['all'],'action':'download','target':None}
        task=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,payload);self.store.set_task(task['id'],state='done',progress=53)
        oldpaths=[r[0] for r in self.store.db.execute('SELECT path FROM cache')]
        await self.submit(snap)
        self.assertEqual(oldpaths,[r[0] for r in self.store.db.execute('SELECT path FROM cache')])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],1)
        self.assertTrue((self.config.root/'评论区').exists())
        self.assertTrue(all(m.download_media.await_count==0 for m in self.client.main+self.client.replies))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM operations').fetchone()[0],0)

    async def test_collision_both_candidates_occupied_fails_closed(self):
        self.source();snap=await self.resolve();selected=snap['main'][0]
        path=self.engine.archive.file(snap,selected);path.parent.mkdir(parents=True);path.write_bytes(b'evil');path.chmod(0o600)
        key=digest([selected['chat'],selected['id'],selected.get('media_id','')])
        alternate=await self.engine.flat_destination(snap,selected,key)
        alternate.write_bytes(b'also');alternate.chmod(0o600)
        payload={'snapshot':snap,'items':[selected],'action':'download','target':None}
        task=self.store.create_task(inbound.OWNER,inbound.OWNER,78001,payload);await self.engine.execute(self.store.claim())
        self.assertEqual(self.store.task(task['id'],inbound.OWNER,inbound.OWNER)['state'],'failed')
        self.assertEqual(path.read_bytes(),b'evil');self.assertEqual(alternate.read_bytes(),b'also')
        self.client.main[0].download_media.assert_not_awaited()

    async def test_missing_main_title_never_falls_back_to_discussion(self):
        self.source();del self.client.channel.title
        with self.assertRaises(Unavailable):await self.resolve()

    async def test_incremental_comments_revalidate_main_authority(self):
        self.source();snap=await self.resolve()
        sub={'snapshot':snap,'thread':snap['thread']}
        with patch('tg_forwarder.control.source.utils.get_peer_id',side_effect=lambda e:e.key):
            new=await self.controller.resolver.new_comments(sub,24144)
        self.assertTrue(all(i['archive_meta']['folder_chat']==source.CHANNEL for i in new['discussion']))
        del self.client.channel.title
        with patch('tg_forwarder.control.source.utils.get_peer_id',side_effect=lambda e:e.key):
            with self.assertRaises(Unavailable):await self.controller.resolver.new_comments(sub,24144)
