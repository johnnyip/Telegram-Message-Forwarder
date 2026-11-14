"""Flat permanent media, original naming, private existing-state metadata; offline."""
from dataclasses import replace
import json
import os
import struct
from pathlib import Path
import unittest
from unittest.mock import patch
import test_cached_resend as fixtures
import test_inbound_photos as inbound
from tg_forwarder.control.config import Config
from tg_forwarder.control.engine import Archive
from tg_forwarder.control.store import digest
from tg_forwarder.canonical_media import filename


class FlatTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown=fixtures.CachedResendTests.asyncTearDown
    original=fixtures.CachedResendTests.original
    execute=fixtures.CachedResendTests.execute
    forbid_source=fixtures.CachedResendTests.forbid_source
    assert_no_source=fixtures.CachedResendTests.assert_no_source
    click=fixtures.CachedResendTests.click
    button=fixtures.CachedResendTests.button
    transport=inbound.InboundTests.transport
    queue=inbound.InboundTests.queue

    async def asyncSetUp(self):
        await fixtures.CachedResendTests.asyncSetUp(self)
        self.config=replace(self.config,archive_flat=True)
        self.engine.config=self.config;self.controller.config=self.config
        self.engine.archive=Archive(self.config.root,0,flat=True,metadata_dir=self.config.db.parent/'manifests')
        self.config.root.chmod(0o777)  # the existing public NFS root, not a requested ACL change

    async def test_mtp_photos_video_all_flat_with_original_names_private_manifests(self):
        row=await self.original(2,video=True)
        payload=json.loads(row['payload'])
        files=[Path(r[0]) for r in self.store.db.execute('SELECT path FROM cache')]
        self.assertEqual(len(files),3)
        self.assertTrue(all(p.parent==self.config.root for p in files))
        self.assertEqual({p.name for p in files},{filename(i) for i in payload['items']})
        self.assertFalse([p for p in self.config.root.iterdir() if p.is_dir()])
        manifest=self.config.db.parent/'manifests'/('manifest_'+row['id']+'.json')
        self.assertTrue(manifest.is_file());self.assertFalse((self.config.root/'.metadata').exists())
        self.assertNotIn('file_id',manifest.read_text());self.assertEqual(manifest.stat().st_mode & 0o077,0)
        self.assertFalse(list(self.config.root.glob('.download-*')))

    async def test_actual_inbound_ten_flat_minor61_names_and_no_source_rpc(self):
        self.transport()
        for mid in range(73263,73273):
            raw=inbound.raw_update(mid)
            header=struct.pack('<ii',2 | (1<<25),2)+bytes([25])+b'R'*25+b'\0'*2
            wire=header+struct.pack('<qq',inbound.PHOTO,12345)+struct.pack('<iii',1,2,ord('y'))+bytes([61,4])
            raw['message']['photo'][-1]['file_id']=inbound.rle(wire)
            await self.queue(raw);row=await self.execute()
            self.assertEqual(row['state'],'done',row['error'])
        paths=[Path(r[0]) for r in self.store.db.execute('SELECT path FROM cache')]
        self.assertEqual(len(paths),10);self.assertTrue(all(p.parent==self.config.root for p in paths))
        self.assertEqual(paths[0].name,'73263 - AgADpsgxGwi2aVU.jpg')
        self.assert_no_source();self.bot.send_photo.assert_not_awaited()
        for manifest in (self.config.db.parent/'manifests').iterdir():
            self.assertNotIn('file_id',manifest.read_text())

    async def test_existing_unknown_same_size_filename_not_adopted_or_overwritten(self):
        from tg_forwarder.control.input import SourceRef
        snap=await self.resolver.resolve(SourceRef('tgfanchabiao',4833));item=snap['main'][0]
        primary=self.engine.archive.file(snap,item);primary.write_bytes(b'evil');primary.chmod(0o600)
        row=await self.original(1)
        saved=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0])
        self.assertNotEqual(saved,primary);self.assertEqual(primary.read_bytes(),b'evil')
        self.assertEqual(saved.read_bytes(),b'data');self.assertIn(' - '+digest([item['chat'],item['id'],item['media_id']])[:16],saved.name)
        self.assertLessEqual(len(saved.name.encode()),230)

    async def test_flat_same_document_name_collision_suffix_is_physical_revision_scoped(self):
        item={'chat':-1000000000011,'id':54,'media_id':'1','kind':'video','archive_meta':{'filename':'a.mp4'}}
        key=digest([item['chat'],54,'1']);one=await self.engine.flat_destination({},item,key)
        one.write_bytes(b'data');self.store.db.execute('INSERT INTO cache VALUES(?,?,?)',(key,str(one),4))
        other={**item,'media_id':'2'};key2=digest([other['chat'],54,'2'])
        two=await self.engine.flat_destination({},other,key2)
        self.assertNotEqual(one,two);self.assertEqual(one.name,'54 - a.mp4')
        self.assertIn(key2[:16],two.name)
        two.write_bytes(b'other')
        with self.assertRaises(ValueError):await self.engine.flat_destination({},other,key2)
        self.assertEqual(two.read_bytes(),b'other')

    async def test_existing_nested_cached_payload_remains_without_migration_or_rpc(self):
        self.engine.archive.flat=False
        row=await self.original(1,video=True);old=[Path(r[0]) for r in self.store.db.execute('SELECT path FROM cache')]
        self.engine.archive.flat=True;self.forbid_source()
        await self.click(self.button());sent=await self.execute()
        self.assertEqual(sent['state'],'done',sent['error']);self.assert_no_source()
        self.assertTrue(all(p.exists() and p.parent!=self.config.root for p in old))

    async def test_symlink_filename_and_unsafe_existing_cache_are_rejected(self):
        item={'chat':1,'id':54,'media_id':'1','kind':'video','archive_meta':{'filename':'a.mp4'}}
        path=self.config.root/'54 - a.mp4';path.symlink_to(self.root/'outside')
        with self.assertRaises(ValueError):await self.engine.flat_destination({},item,'key')
        path.unlink();path.write_bytes(b'data');path.chmod(0o666)
        self.store.db.execute('INSERT INTO cache VALUES(?,?,?)',(digest([1,54,'1']),str(path),4))
        with self.assertRaises(ValueError):await self.engine.cached_file_async(item)

    async def test_owned_temp_inode_and_atomic_no_clobber(self):
        fd,temp=await self.engine.download_temporary(self.config.root)
        info=os.fstat(fd);os.write(fd,b'data');os.close(fd)
        identity=(info.st_dev,info.st_ino);target=self.config.root/'target.jpg'
        target.write_bytes(b'other')
        with self.assertRaises(FileExistsError):self.engine.finalize(temp,target,identity)
        self.assertEqual(target.read_bytes(),b'other')
        # Keep the original inode alive while replacing its public name.
        held=os.open(temp,os.O_RDONLY);Path(temp).unlink();Path(temp).write_bytes(b'foreign')
        try:
            with self.assertRaises(ValueError):self.engine.finalize(temp,self.config.root/'new.jpg',identity)
            self.assertFalse((self.config.root/'new.jpg').exists());self.assertEqual(Path(temp).read_bytes(),b'foreign')
        finally:os.close(held)

    async def test_actual_download_cleanup_does_not_unlink_replaced_temp(self):
        from unittest.mock import AsyncMock
        async def replace_temp(file,progress_callback):
            file.write(b'data');file.flush()
            temporary=next(self.config.root.glob('.download-*'))
            temporary.unlink();temporary.write_bytes(b'evil')
            return 'downloaded'
        self.client.main[0].download_media=AsyncMock(side_effect=replace_temp)
        from tg_forwarder.control.input import SourceRef
        snap=await self.resolver.resolve(SourceRef('tgfanchabiao',4833))
        payload={'snapshot':snap,'items':[snap['main'][0]],'action':'download','target':None}
        self.store.create_task(fixtures.OWNER,fixtures.OWNER,100,payload)
        row=await self.execute();self.assertNotEqual(row['state'],'done')
        leftovers=list(self.config.root.glob('.download-*'))
        self.assertEqual(len(leftovers),1);self.assertEqual(leftovers[0].read_bytes(),b'evil')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM cache').fetchone()[0],0)

    async def test_private_metadata_rejects_public_or_symlink_state(self):
        archive=Archive(self.config.root,0,flat=True,metadata_dir=self.config.root/'metadata')
        with self.assertRaises(ValueError):archive.manifest({},'task',{})
        self.engine.archive.metadata_dir.mkdir(mode=0o700)
        self.engine.archive.metadata_dir.chmod(0o777)
        with self.assertRaises(ValueError):self.engine.archive.manifest({},'task',{})


class FlatConfigTests(unittest.TestCase):
    def test_explicit_strict_switch_other_roles_default_unchanged(self):
        with patch.dict(os.environ,{},clear=True):self.assertFalse(Config.from_env().archive_flat)
        with patch.dict(os.environ,{'CONTROL_ARCHIVE_FLAT':'true'},clear=True):self.assertTrue(Config.from_env().archive_flat)
        with patch.dict(os.environ,{'CONTROL_ARCHIVE_FLAT':'1'},clear=True):
            with self.assertRaises(ValueError):Config.from_env()
