import os
import unittest

from tg_forwarder.runtime.bot_limits import (
    DEFAULT_CLOUD_BOT_UPLOAD_MAX_MB,
    DEFAULT_LOCAL_BOT_UPLOAD_MAX_MB,
    resolve_bot_album_max_total_mb,
    resolve_bot_upload_max_mb,
)


class BotLimitsTests(unittest.TestCase):
    def setUp(self):
        self._saved = {
            "BOT_UPLOAD_MAX_MB": os.environ.get("BOT_UPLOAD_MAX_MB"),
            "BOT_ALBUM_MAX_TOTAL_MB": os.environ.get("BOT_ALBUM_MAX_TOTAL_MB"),
        }

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_cloud_endpoint_defaults_to_50mb(self):
        os.environ.pop("BOT_UPLOAD_MAX_MB", None)
        self.assertEqual(
            resolve_bot_upload_max_mb("https://api.telegram.org/bot", bot_local_mode=False),
            DEFAULT_CLOUD_BOT_UPLOAD_MAX_MB,
        )

    def test_custom_endpoint_defaults_to_local_limit(self):
        os.environ.pop("BOT_UPLOAD_MAX_MB", None)
        self.assertEqual(
            resolve_bot_upload_max_mb("http://bot-api:8081/bot", bot_local_mode=False),
            DEFAULT_LOCAL_BOT_UPLOAD_MAX_MB,
        )

    def test_local_mode_defaults_to_local_limit_even_with_official_base(self):
        os.environ.pop("BOT_UPLOAD_MAX_MB", None)
        self.assertEqual(
            resolve_bot_upload_max_mb("https://api.telegram.org/bot", bot_local_mode=True),
            DEFAULT_LOCAL_BOT_UPLOAD_MAX_MB,
        )

    def test_explicit_env_override_wins(self):
        os.environ["BOT_UPLOAD_MAX_MB"] = "123"
        self.assertEqual(
            resolve_bot_upload_max_mb("https://api.telegram.org/bot", bot_local_mode=False),
            123,
        )

    def test_album_limit_follows_upload_limit_when_unset(self):
        os.environ.pop("BOT_UPLOAD_MAX_MB", None)
        os.environ.pop("BOT_ALBUM_MAX_TOTAL_MB", None)
        self.assertEqual(
            resolve_bot_album_max_total_mb("https://api.telegram.org/bot", bot_local_mode=False),
            DEFAULT_CLOUD_BOT_UPLOAD_MAX_MB,
        )

    def test_album_limit_explicit_override_wins(self):
        os.environ["BOT_ALBUM_MAX_TOTAL_MB"] = "321"
        self.assertEqual(
            resolve_bot_album_max_total_mb("https://api.telegram.org/bot", bot_local_mode=False),
            321,
        )


if __name__ == "__main__":
    unittest.main()
