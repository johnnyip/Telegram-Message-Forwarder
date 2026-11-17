import ast
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from tg_forwarder import spool_lifecycle as sl
from tg_forwarder.delivery import pending_edits as pe
from tg_forwarder.delivery import edit_updates as eu


class PendingEditSafetyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "spool"
        self.root.mkdir()
        sl.configure(self.root)
        self.addCleanup(sl._MANAGERS.pop, self.root, None)
        self.routes = [{"name": "r", "sources": {1}, "targets": [7]}]
        self.info = {"chat_id": 1, "msg_id": 2}

    async def test_policy_change_fences_queued_edit_before_mapping_lookup(self):
        policy = pe.route_signature(self.routes, 1, {"photo"}, {"blocked"}, {42})
        pe.enqueue(self.info, "new text", "text", policy)
        lookup = AsyncMock(return_value={"targets": {"7": {}}})
        with patch.object(eu, "get_message_mapping", lookup):
            await pe.replay_once(None, lambda _: None, self.routes, None, {"photo"}, {"newly-blocked"}, {42})
        lookup.assert_not_awaited()
        with sl.manager().db() as c:
            self.assertEqual(c.execute("SELECT state FROM pending_edits").fetchone()[0], "policy_changed")

    def test_edited_handler_enqueues_without_redis_availability_gate(self):
        tree = ast.parse(Path("main.py").read_text())
        handler = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "on_message_edited")
        source = ast.get_source_segment(Path("main.py").read_text(), handler)
        self.assertIn("enqueue", source)
        self.assertNotIn("get_redis", source)
