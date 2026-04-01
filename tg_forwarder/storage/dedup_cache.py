import os
import time
from typing import Dict

DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "900"))
DEDUP_CACHE: Dict[str, float] = {}


def _now() -> float:
    return time.time()


def _purge_expired() -> None:
    now = _now()
    expired = [key for key, ts in DEDUP_CACHE.items() if now - ts > DEDUP_TTL_SECONDS]
    for key in expired:
        DEDUP_CACHE.pop(key, None)


def dedup_seen(key: str) -> bool:
    _purge_expired()
    ts = DEDUP_CACHE.get(key)
    if ts is None:
        return False
    return _now() - ts <= DEDUP_TTL_SECONDS


def dedup_mark(key: str) -> None:
    _purge_expired()
    DEDUP_CACHE[key] = _now()


__all__ = [
    "DEDUP_CACHE",
    "DEDUP_TTL_SECONDS",
    "dedup_mark",
    "dedup_seen",
]
