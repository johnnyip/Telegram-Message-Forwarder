import logging
import os

ENABLE_DEBUG_LOGS = os.getenv("ENABLE_DEBUG_LOGS", "false").strip().lower() in {"1", "true", "yes", "y"}
TELETHON_DEBUG = os.getenv("TELETHON_DEBUG", "false").strip().lower() in {"1", "true", "yes", "y"}


def maybe_debug_log(log_fn, payload: dict):
    if ENABLE_DEBUG_LOGS:
        log_fn(payload)


def configure_telethon_logger():
    level = logging.DEBUG if TELETHON_DEBUG else logging.INFO
    logging.getLogger("telethon").setLevel(level)
    logging.getLogger("telethon.network").setLevel(level)
    logging.getLogger("telethon.network.mtprotosender").setLevel(level)
