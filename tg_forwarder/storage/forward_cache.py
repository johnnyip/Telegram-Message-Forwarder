import time
from typing import Dict, Optional

FORWARD_CACHE_TTL_SECONDS = 3600
FORWARD_CAP_CACHE: Dict[int, tuple[float, bool]] = {}


def _now() -> float:
    return time.time()


def forward_cache_get(chat_id: int) -> Optional[bool]:
    item = FORWARD_CAP_CACHE.get(chat_id)
    if not item:
        return None
    ts, value = item
    if _now() - ts > FORWARD_CACHE_TTL_SECONDS:
        FORWARD_CAP_CACHE.pop(chat_id, None)
        return None
    return value


def forward_cache_set(chat_id: int, value: bool):
    FORWARD_CAP_CACHE[chat_id] = (_now(), value)


__all__ = [
    "FORWARD_CAP_CACHE",
    "FORWARD_CACHE_TTL_SECONDS",
    "forward_cache_get",
    "forward_cache_set",
]
