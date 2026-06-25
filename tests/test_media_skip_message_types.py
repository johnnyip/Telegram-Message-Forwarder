import unittest
from types import SimpleNamespace

from tg_forwarder.domain.media import skip_message_kind


class MediaSkipMessageTypesTests(unittest.TestCase):
    def test_skip_text_message_when_configured(self):
        msg = SimpleNamespace(media=None, photo=None, video=None, video_note=None, voice=None, audio=None, animation=None, document=None)
        self.assertEqual(skip_message_kind(msg, {"text"}), "text")

    def test_skip_photo_message_when_configured(self):
        msg = SimpleNamespace(media=object(), photo=object(), video=None, video_note=None, voice=None, audio=None, animation=None, document=None)
        self.assertEqual(skip_message_kind(msg, {"image"}), "image")

    def test_do_not_skip_video_when_image_skip_enabled(self):
        msg = SimpleNamespace(media=object(), photo=None, video=object(), video_note=None, voice=None, audio=None, animation=None, document=None)
        self.assertIsNone(skip_message_kind(msg, {"image"}))


if __name__ == "__main__":
    unittest.main()
