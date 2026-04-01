import os

VERBOSE_JOB_LOGS = os.getenv("VERBOSE_JOB_LOGS", "false").strip().lower() in {"1", "true", "yes", "y"}
BOT_STARTUP_SMOKE_TEST = os.getenv("BOT_STARTUP_SMOKE_TEST", "false").strip().lower() in {"1", "true", "yes", "y"}


def maybe_verbose_log(log_fn, payload: dict):
    if VERBOSE_JOB_LOGS:
        log_fn(payload)


__all__ = [
    "BOT_STARTUP_SMOKE_TEST",
    "VERBOSE_JOB_LOGS",
    "maybe_verbose_log",
]
