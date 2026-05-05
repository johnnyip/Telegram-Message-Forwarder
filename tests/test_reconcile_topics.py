import sys
import types
import unittest
from types import SimpleNamespace

redis_mod = types.ModuleType("redis")
redis_asyncio_mod = types.ModuleType("redis.asyncio")
redis_asyncio_mod.Redis = object
redis_mod.asyncio = redis_asyncio_mod
sys.modules.setdefault("redis", redis_mod)
sys.modules.setdefault("redis.asyncio", redis_asyncio_mod)

from tg_forwarder.reconcile_topics import _build_forward_batches, _parse_target_chat_ids


class ReconcileTopicsBatchingTest(unittest.TestCase):
    def test_build_forward_batches_splits_plain_messages_at_batch_size(self):
        msgs = [SimpleNamespace(id=i, grouped_id=None) for i in range(1, 121)]
        batches = _build_forward_batches(msgs, 50)
        self.assertEqual([len(b) for b in batches], [50, 50, 20])

    def test_build_forward_batches_keeps_same_album_together(self):
        msgs = [SimpleNamespace(id=i, grouped_id=None) for i in range(1, 51)]
        msgs.extend(SimpleNamespace(id=i, grouped_id=999) for i in range(51, 56))
        batches = _build_forward_batches(msgs, 50)
        self.assertEqual([len(b) for b in batches], [50, 5])

    def test_parse_target_chat_ids_supports_plural_and_single_env(self):
        import tg_forwarder.reconcile_topics as mod
        old_single = mod.RECONCILE_TARGET_CHAT_ID_RAW
        old_multi = mod.RECONCILE_TARGET_CHAT_IDS_RAW
        try:
            mod.RECONCILE_TARGET_CHAT_ID_RAW = '-1001'
            mod.RECONCILE_TARGET_CHAT_IDS_RAW = '-1002, -1003'
            self.assertEqual(_parse_target_chat_ids(), [-1002, -1003, -1001])
        finally:
            mod.RECONCILE_TARGET_CHAT_ID_RAW = old_single
            mod.RECONCILE_TARGET_CHAT_IDS_RAW = old_multi


if __name__ == "__main__":
    unittest.main()
