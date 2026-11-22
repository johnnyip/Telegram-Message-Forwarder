import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch, AsyncMock
from tg_forwarder.spool_lifecycle import Spool
from tg_forwarder.spool_import import relative_path, archive_file, PREFIX, request, ImportQueue

class ParseTests(unittest.TestCase):
    def test_notice(self):
        rel='new/7750751422/'+'a'*32+'_939528_movie.mp4'
        self.assertEqual(relative_path('Saved locally:\n'+PREFIX+rel+'\nOriginal send 00:45'),rel)
        self.assertIsNone(relative_path('https://t.me/example/1'))
    def test_reject_unsafe_and_multiple(self):
        for value in ['../secret','new/1/../../a','new/1/'+'a'*32+'_a/evil','new/1/'+'a'*32+'_a\\evil','new/1/'+'a'*32+'_.part\x00']:
            with self.assertRaises(ValueError):relative_path(PREFIX+value)
        with self.assertRaises(ValueError):relative_path(PREFIX+'x\n'+PREFIX+'y')
    def test_principal_bound_key(self):
        rel='new/1/'+'a'*32+'_movie.mp4'
        self.assertNotEqual(request(1,rel)[0],request(2,rel)[0])
        with self.assertRaises(ValueError):request(True,rel)

class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.spool=Spool(Path(self.tmp.name)/'spool');self.archive=Path(self.tmp.name)/'downloads';self.archive.mkdir()
        self.token,fd,_=self.spool.allocate('new',1,'movie.mp4');os.write(fd,b'original-video');os.close(fd)
        self.source=self.spool.finalize(self.token,True)
        self.job={'source_kind':'new','route_names':['route'],'info':{},'snapshots':[{'path':str(self.source),'spool_token':self.token}]}
        self.spool.bind(self.job,[['route',-2]]);self.spool.start(self.job,[['route',-2]],True)
        self.relative=str(self.source.relative_to(self.spool.root))
    def terminal(self):
        self.spool.begin_target(self.job,'route',-2)
        self.spool.target_result(self.job,'route',-2,NS(delivery_kind='file',sent_message_id=10,preserve_local_copy=False))
        self.assertTrue(self.spool.finish(self.job,[str(self.source)]))
    def run_import(self):return archive_file(self.spool.root,self.archive,self.relative,reserve=0)
    def test_move_terminal_and_repeat(self):
        self.terminal();result=self.run_import()
        self.assertEqual(result['state'],'moved');self.assertFalse(self.source.exists())
        self.assertEqual((self.archive/result['name']).read_bytes(),b'original-video')
        self.assertEqual(self.run_import(),result)
        self.assertTrue(self.spool.complete(self.job))
    def test_pending_is_not_deleted_or_falsely_completed(self):
        result=self.run_import();self.assertEqual(result['state'],'copied_pending');self.assertTrue(self.source.exists())
        self.assertFalse(self.spool.complete(self.job))
        self.terminal();self.assertEqual(self.run_import()['state'],'moved')
    def test_unknown_file_not_adopted(self):
        with self.assertRaises(ValueError):archive_file(self.spool.root,self.archive,'new/1/'+'f'*32+'_unknown.mp4',reserve=0)
        self.assertTrue(self.source.exists())
    def test_changed_source_retained(self):
        self.terminal();self.source.write_bytes(b'foreign!')
        with self.assertRaises(ValueError):self.run_import()
        self.assertEqual(self.source.read_bytes(),b'foreign!')
    def test_symlink_source_rejected(self):
        self.terminal();other=self.source.with_name('other');self.source.rename(other);self.source.symlink_to(other)
        with self.assertRaises((OSError,ValueError)):self.run_import()
        self.assertTrue(other.exists())
    def test_hardlink_source_rejected(self):
        self.terminal();os.link(self.source,self.source.with_name('other'))
        with self.assertRaises(ValueError):self.run_import()
        self.assertTrue(self.source.exists())
    def test_destination_conflict_never_clobbered(self):
        self.terminal();dest=self.archive/self.source.name;dest.write_bytes(b'other-file')
        with self.assertRaises(ValueError):self.run_import()
        self.assertEqual(dest.read_bytes(),b'other-file');self.assertTrue(self.source.exists())
    def test_destination_symlink_rejected(self):
        self.terminal();other=self.archive/'other';other.write_bytes(b'original-video');(self.archive/self.source.name).symlink_to(other)
        with self.assertRaises(OSError):self.run_import()
        self.assertTrue(self.source.exists());self.assertEqual(other.read_bytes(),b'original-video')
    def test_missing_mount_not_created(self):
        missing=self.archive/'not-mounted'
        with self.assertRaises(FileNotFoundError):archive_file(self.spool.root,missing,self.relative,reserve=0)
        self.assertFalse(missing.exists());self.assertTrue(self.source.exists())
    def test_checksum_corruption_prevents_cleanup(self):
        self.run_import();dest=self.archive/self.source.name;dest.write_bytes(b'corrupt!');self.terminal()
        with self.assertRaises(ValueError):self.run_import()
        self.assertTrue(self.source.exists())
    def test_crash_after_publish_before_receipt_is_reconciled(self):
        self.terminal();original=self.spool.put
        # Simulate the process dying before verified destination receipt commit.
        from tg_forwarder import spool_lifecycle
        def fail(spool,c,row):
            if row.get('archive_export',{}).get('state')=='verified':raise RuntimeError('crash')
            original(c,row)
        with patch.object(spool_lifecycle.Spool,'put',fail):
            with self.assertRaises(RuntimeError):self.run_import()
        self.assertTrue(self.source.exists());self.assertEqual(self.run_import()['state'],'moved')
    def test_no_space_preserves_source(self):
        with patch('tg_forwarder.spool_import.shutil.disk_usage',return_value=NS(free=0)):
            with self.assertRaises(ValueError):self.run_import()
        self.assertTrue(self.source.exists());self.assertEqual(list(self.archive.iterdir()),[])

class QueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_required(self):
        redis=NS(get=AsyncMock(return_value=None));q=ImportQueue(NS(redis=redis,prefix='test:'))
        with self.assertRaises(ValueError):await q.submit(1,'new/1/'+'a'*32+'_a.mp4')
    async def test_intake_cap_is_reported(self):
        redis=NS(get=AsyncMock(return_value='1'),eval=AsyncMock(return_value=-1));q=ImportQueue(NS(redis=redis,prefix='test:'))
        with self.assertRaises(ValueError):await q.submit(1,'new/1/'+'a'*32+'_a.mp4')
    async def test_unauthorized_request_does_not_touch_files(self):
        ident,value=request(2,'new/1/'+'a'*32+'_a.mp4')
        redis=NS(zrange=AsyncMock(return_value=[ident]),get=AsyncMock(return_value=json.dumps(value)),set=AsyncMock(),expire=AsyncMock(),zrem=AsyncMock())
        q=ImportQueue(NS(redis=redis,prefix='test:'))
        with patch('tg_forwarder.spool_import.archive_file') as move:
            await q.tick('/spool','/archive',{1});move.assert_not_called()
        self.assertEqual(json.loads(redis.set.call_args.args[1])['state'],'error')

class ControllerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from tg_forwarder.control.config import Config
        from tg_forwarder.control.store import Store
        from tg_forwarder.control.controller import Controller
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'control.db');self.addCleanup(self.store.close)
        self.bot=NS(send_message=AsyncMock(return_value=NS(message_id=50)),edit_message_text=AsyncMock())
        self.config=Config(frozenset({1}),db=Path(self.tmp.name)/'control.db')
        self.controller=Controller(self.config,self.store,NS(resolve=AsyncMock()),NS(bridge=None),self.bot)
    def update(self,owner=1,kind='private'):
        from telegram import Update
        return Update.de_json({'update_id':5,'message':{'message_id':10,'date':100,'from':{'id':owner,'is_bot':False,'first_name':'owner'},'chat':{'id':owner,'type':kind},'text':'Saved locally:\n'+PREFIX+'new/1/'+'a'*32+'_movie.mp4'}},None)
    async def test_authorized_intake_and_explicit_unconfigured_error(self):
        await self.controller.handle(self.update())
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM spool_imports').fetchone()[0],1)
        await self.controller.spool_import_tick()
        self.assertIn('spool 保留',self.bot.edit_message_text.call_args.kwargs['text'])
    async def test_unauthorized_actor_does_not_parse_or_reply(self):
        with patch('tg_forwarder.spool_import.relative_path',side_effect=AssertionError('must not parse')):
            await self.controller.handle(self.update(2))
        self.bot.send_message.assert_not_called()
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM spool_imports').fetchone()[0],0)
    async def test_group_cannot_import(self):
        await self.controller.handle(self.update(kind='group'))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM spool_imports').fetchone()[0],0)
    async def test_same_update_is_not_registered_twice(self):
        await self.controller.handle(self.update());await self.controller.handle(self.update())
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM spool_imports').fetchone()[0],1)
        self.bot.send_message.assert_awaited_once()

    async def test_rate_limit_honors_retry_after(self):
        import time
        from telegram.error import RetryAfter
        from unittest.mock import Mock
        await self.controller.handle(self.update())
        self.controller.engine.gate=NS(delay=Mock())
        self.bot.edit_message_text.side_effect=RetryAfter(20)
        await self.controller.spool_import_tick()
        row=self.store.db.execute('SELECT state,due FROM spool_imports').fetchone()
        self.assertEqual(row['state'],'pending');self.assertGreater(row['due'],time.time()+18)
        await self.controller.spool_import_tick()
        self.assertEqual(self.bot.edit_message_text.await_count,1)
