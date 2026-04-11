import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional

SEND_JOURNAL_PATH = Path(os.getenv("SEND_JOURNAL_PATH", "./downloads/send_journal.json")).expanduser()
SEND_JOURNAL_TTL_SECONDS = int(os.getenv("SEND_JOURNAL_TTL_SECONDS", str(7 * 24 * 3600)))
SEND_JOURNAL_INFLIGHT_TTL_SECONDS = int(os.getenv("SEND_JOURNAL_INFLIGHT_TTL_SECONDS", "900"))
# After this many consecutive failures for the same fingerprint, the consumer
# will skip ("poison-message" skip) and commit the offset to unblock the
# Kafka partition (P12).
SEND_JOURNAL_MAX_FAILURES = int(os.getenv("SEND_JOURNAL_MAX_FAILURES", "5"))


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
    """Write journal atomically via temp-file + os.replace() (P10).

    This prevents a mid-write process kill from leaving a corrupt JSON file.
    os.replace() is atomic on POSIX when src and dst are on the same filesystem.
    """
    SEND_JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=SEND_JOURNAL_PATH.parent,
        prefix=".send_journal_tmp_",
        suffix=".json",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp_path, SEND_JOURNAL_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise


def _is_entry_alive(value: Dict[str, Any], now: float) -> bool:
    ts = value.get("ts")
    if not isinstance(ts, (int, float)):
        return False
    status = value.get("status")
    ttl = SEND_JOURNAL_INFLIGHT_TTL_SECONDS if status == "processing" else SEND_JOURNAL_TTL_SECONDS
    return now - float(ts) <= ttl


def _purge(data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    now = _now()
    kept: Dict[str, Dict[str, Any]] = {}
    for key, value in data.items():
        if isinstance(value, dict) and _is_entry_alive(value, now):
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


def journal_claim(key: str, payload: Optional[Dict[str, Any]] = None) -> bool:
    data = _purge(_load())
    existing = data.get(key)
    if isinstance(existing, dict) and existing.get("status") in {"processing", "done"}:
        _save(data)
        return False
    data[key] = {
        "ts": _now(),
        "status": "processing",
        **(payload or {}),
    }
    _save(data)
    return True


def journal_record_failure(key: str, payload: Optional[Dict[str, Any]] = None) -> int:
    """Record a processing failure for this key and return the new total count (P12).

    The entry is written with status='failed' so that journal_claim will not
    block future retries (only 'processing' and 'done' statuses block claims).
    """
    data = _purge(_load())
    existing = data.get(key, {})
    failures = int(existing.get("failures", 0)) + 1
    data[key] = {
        "ts": _now(),
        "status": "failed",
        "failures": failures,
        **(payload or {}),
    }
    _save(data)
    return failures


def journal_is_poison(key: str) -> bool:
    """Return True if this key has exceeded SEND_JOURNAL_MAX_FAILURES (P12)."""
    data = _purge(_load())
    existing = data.get(key)
    if not isinstance(existing, dict):
        return False
    return int(existing.get("failures", 0)) >= SEND_JOURNAL_MAX_FAILURES


__all__ = [
    "SEND_JOURNAL_INFLIGHT_TTL_SECONDS",
    "SEND_JOURNAL_MAX_FAILURES",
    "SEND_JOURNAL_PATH",
    "SEND_JOURNAL_TTL_SECONDS",
    "journal_claim",
    "journal_get",
    "journal_is_poison",
    "journal_mark",
    "journal_record_failure",
]
