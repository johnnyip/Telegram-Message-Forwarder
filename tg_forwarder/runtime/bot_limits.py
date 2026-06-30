import os

DEFAULT_CLOUD_BOT_UPLOAD_MAX_MB = 50
DEFAULT_LOCAL_BOT_UPLOAD_MAX_MB = 2000


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def is_official_bot_api_base(base_url: str) -> bool:
    value = (base_url or "").strip().lower()
    return not value or value.startswith("https://api.telegram.org/")


def resolve_bot_upload_max_mb(bot_api_base: str, *, bot_local_mode: bool) -> int:
    raw = os.getenv("BOT_UPLOAD_MAX_MB")
    if raw:
        return int(raw)
    if bot_local_mode or not is_official_bot_api_base(bot_api_base):
        return DEFAULT_LOCAL_BOT_UPLOAD_MAX_MB
    return DEFAULT_CLOUD_BOT_UPLOAD_MAX_MB


def resolve_bot_album_max_total_mb(bot_api_base: str, *, bot_local_mode: bool) -> int:
    raw = os.getenv("BOT_ALBUM_MAX_TOTAL_MB")
    if raw:
        return int(raw)
    return resolve_bot_upload_max_mb(bot_api_base, bot_local_mode=bot_local_mode)


__all__ = [
    "DEFAULT_CLOUD_BOT_UPLOAD_MAX_MB",
    "DEFAULT_LOCAL_BOT_UPLOAD_MAX_MB",
    "env_flag",
    "is_official_bot_api_base",
    "resolve_bot_album_max_total_mb",
    "resolve_bot_upload_max_mb",
]
