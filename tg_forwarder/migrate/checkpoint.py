"""
Checkpoint management for APP_MODE=migrate.

Written atomically (temp-file + os.replace) so a mid-write crash never
leaves a corrupt file.  Fields are append-only safe — unknown keys are
preserved on round-trip.
"""

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional


MIGRATE_CHECKPOINT_PATH = Path(
    os.getenv("MIGRATE_CHECKPOINT_PATH", "./downloads/migrate_checkpoint.json")
).expanduser()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())


def _save(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".migrate_chk_tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise


def load_checkpoint(path: Optional[Path] = None) -> Dict[str, Any]:
    p = path or MIGRATE_CHECKPOINT_PATH
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_checkpoint(
    *,
    path: Optional[Path] = None,
    chat_id: int,
    thread_id: int,
    last_processed_msg_id: int,
    processed_count: int,
    migrated_count: int,
    skipped_count: int,
    migration_topic_id: Optional[int],
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    p = path or MIGRATE_CHECKPOINT_PATH
    existing = load_checkpoint(p)
    data = {
        **existing,
        "chat_id": chat_id,
        "thread_id": thread_id,
        "last_processed_msg_id": last_processed_msg_id,
        "processed_count": processed_count,
        "migrated_count": migrated_count,
        "skipped_count": skipped_count,
        "migration_topic_id": migration_topic_id,
        "updated_at": _now_iso(),
        **(extra or {}),
    }
    if "started_at" not in data:
        data["started_at"] = _now_iso()
    _save(p, data)


__all__ = [
    "MIGRATE_CHECKPOINT_PATH",
    "load_checkpoint",
    "save_checkpoint",
]
