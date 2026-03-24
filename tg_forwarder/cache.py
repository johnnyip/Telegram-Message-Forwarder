import time
from typing import Any, Dict, Optional


CACHE_TTL_SECONDS = 3600
CHAT_INFO_CACHE: Dict[Any, tuple[float, dict]] = {}
SENDER_INFO_CACHE: Dict[Any, tuple[float, dict]] = {}


def _now() -> float:
    return time.time()


def cache_get(store: Dict[Any, tuple[float, dict]], key: Any) -> Optional[dict]:
    item = store.get(key)
    if not item:
        return None
    ts, value = item
    if _now() - ts > CACHE_TTL_SECONDS:
        store.pop(key, None)
        return None
    return value.copy()


def cache_set(store: Dict[Any, tuple[float, dict]], key: Any, value: dict):
    store[key] = (_now(), value.copy())
