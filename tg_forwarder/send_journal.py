import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

SEND_JOURNAL_PATH = Path(os.getenv("SEND_JOURNAL_PATH", "./downloads/send_journal.json")).expanduser()
SEND_JOURNAL_TTL_SECONDS = int(os.getenv("SEND_JOURNAL_TTL_SECONDS", str(7 * 24 * 3600)))


def _now() -> float:
    return time.time()


def _load() -> Dict[str, Dict[str, Any]]:
    if not SEND_JOURNAL_PATH.exists():
        return {}
    try:
        return json.loads(SEND_JOURNAL_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(data: Dict[str, Dict[str, Any]]) -> None:
    SEND_JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    SEND_JOURNAL_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _purge(data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    now = _now()
    kept: Dict[str, Dict[str, Any]] = {}
    for key, value in data.items():
        ts = value.get("ts")
        if isinstance(ts, (int, float)) and now - float(ts) <= SEND_JOURNAL_TTL_SECONDS:
            kept[key] = value
    return kept


def journal_get(key: str) -> Optional[Dict[str, Any]]:
    data = _purge(_load())
    if data:
        _save(data)
    return data.get(key)


def journal_mark(key: str, payload: Dict[str, Any]) -> None:
    data = _purge(_load())
    data[key] = {
        "ts": _now(),
        **payload,
    }
    _save(data)
