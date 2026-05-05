import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from tg_forwarder.forum_topics import parse_sender_id_from_topic_title, pick_latest_topic


def _topic(topic_id: int, title: str, *, ts: int, top_message: int):
    return SimpleNamespace(
        id=topic_id,
        title=title,
        date=datetime.fromtimestamp(ts, tz=timezone.utc),
        top_message=top_message,
    )


class ForumTopicsTest(unittest.TestCase):
    def test_parse_sender_id_from_topic_title(self):
        self.assertEqual(parse_sender_id_from_topic_title("@alice (123456)"), 123456)
        self.assertEqual(parse_sender_id_from_topic_title("Alice Bob (42)"), 42)
        self.assertIsNone(parse_sender_id_from_topic_title("No numeric suffix"))

    def test_pick_latest_topic_prefers_newest_date_then_message(self):
        older = _topic(100, "@alice (123)", ts=1000, top_message=10)
        newer = _topic(101, "@alice (123)", ts=2000, top_message=9)
        self.assertEqual(pick_latest_topic([older, newer]).id, 101)

        same_time_low = _topic(200, "@alice (123)", ts=2000, top_message=11)
        same_time_high = _topic(201, "@alice (123)", ts=2000, top_message=12)
        self.assertEqual(pick_latest_topic([same_time_low, same_time_high]).id, 201)



if __name__ == "__main__":
    unittest.main()
