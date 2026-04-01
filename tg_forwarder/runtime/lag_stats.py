from collections import defaultdict, deque
from typing import Deque, Dict

LAG_WINDOW = 200
lag_by_chat: Dict[int, Deque[int]] = defaultdict(lambda: deque(maxlen=LAG_WINDOW))


def lag_record(chat_id: int, lag_ms: int | None):
    if chat_id is None or lag_ms is None:
        return
    lag_by_chat[chat_id].append(int(lag_ms))


def lag_summary(chat_id: int):
    items = list(lag_by_chat.get(chat_id, []))
    if not items:
        return None
    items_sorted = sorted(items)
    n = len(items_sorted)
    p95 = items_sorted[min(n - 1, max(0, int(n * 0.95) - 1))]
    return {
        "samples": n,
        "avg_ms": int(sum(items_sorted) / n),
        "max_ms": items_sorted[-1],
        "p95_ms": p95,
    }


__all__ = [
    "LAG_WINDOW",
    "lag_record",
    "lag_summary",
]
