import unittest
from types import SimpleNamespace

from tg_forwarder.domain.forwarding_policy import should_process_edit_update
from tg_forwarder.domain.routes import load_routes


class EditRouteFilterTests(unittest.TestCase):
    def setUp(self):
        self.routes = load_routes(
            '[{"name":"media","sources":[-1001],"targets":[-1002]},'
            '{"name":"text","sources":[-1003],"targets":[-1002]}]'
        )
        self.photo = SimpleNamespace(
            media=object(), photo=object(), video=None, video_note=None,
            voice=None, audio=None, animation=None, document=None,
        )
        self.text = SimpleNamespace(
            media=None, photo=None, video=None, video_note=None,
            voice=None, audio=None, animation=None, document=None,
        )

    def test_configured_source_is_eligible_for_edit_mapping_lookup(self):
        self.assertTrue(should_process_edit_update(
            {"chat_id": -1001}, self.photo, self.routes, {"text"}, set(), set()
        ))

    def test_target_only_chat_is_not_eligible_for_edit_mapping_lookup(self):
        self.assertFalse(should_process_edit_update(
            {"chat_id": -1002}, self.photo, self.routes, set(), set(), set()
        ))

    def test_unrelated_chat_is_not_eligible_for_edit_mapping_lookup(self):
        self.assertFalse(should_process_edit_update(
            {"chat_id": -1004}, self.photo, self.routes, set(), set(), set()
        ))

    def test_message_type_skipped_on_initial_receive_is_not_eligible(self):
        self.assertFalse(should_process_edit_update(
            {"chat_id": -1001}, self.text, self.routes, {"text"}, set(), set()
        ))

    def test_ignored_sender_is_not_eligible(self):
        info = {"chat_id": -1001, "sender_id": 42, "sender_username": "alice"}
        self.assertFalse(should_process_edit_update(
            info, self.photo, self.routes, set(), {"alice"}, {42}
        ))


if __name__ == "__main__":
    unittest.main()
