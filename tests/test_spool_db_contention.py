import multiprocessing as mp
from pathlib import Path
import tempfile
import time
import unittest
from tg_forwarder.spool_lifecycle import Spool

def hold(root,ready):
    with Spool(root).db() as c:
        c.execute('INSERT INTO files VALUES (?,?)',('held','{}'))
        ready.set();time.sleep(6)

class ContentionTests(unittest.TestCase):
    def test_process_waits_beyond_old_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=Spool(tmp)
            with m.db():pass
            ctx=mp.get_context('spawn');ready=ctx.Event();p=ctx.Process(target=hold,args=(tmp,ready));p.start()
            try:
                self.assertTrue(ready.wait(10));start=time.monotonic()
                with m.db() as c:self.assertIsNotNone(c.execute('SELECT data FROM files WHERE token=?',('held',)).fetchone())
                self.assertGreater(time.monotonic()-start,5);p.join(10);self.assertEqual(p.exitcode,0)
            finally:
                if p.is_alive():p.terminate();p.join()
    def test_rollback_releases_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=Spool(tmp)
            with self.assertRaises(ValueError):
                with m.db() as c:
                    c.execute('INSERT INTO files VALUES (?,?)',('abort','{}'));raise ValueError('abort')
            with m.db() as c:self.assertIsNone(c.execute('SELECT data FROM files WHERE token=?',('abort',)).fetchone())
    def test_symlink_lock_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=Spool(tmp);(m.guard()/'db.lock').symlink_to(Path(tmp)/'foreign')
            with self.assertRaises(OSError):
                with m.db():pass
