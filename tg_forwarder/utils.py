import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


def tstamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def now_ts() -> float:
    return time.time()


def log(obj: dict, log_dir: Path):
    line = json.dumps(obj, ensure_ascii=False, default=str)
    print(line)
    try:
        today_file = log_dir / f"{datetime.now().strftime('%Y-%m-%d')}.log"
        with today_file.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception as e:
        print(f"[LOG_ERR] {e}")


def safe_str(value) -> str:
    return "null" if value is None or value == "" else str(value)


def sanitize_filename_part(value: str) -> str:
    if not value:
        return "unknown"
    cleaned = "".join(c for c in str(value) if c.isalnum() or c in ("@", "_", "-", ".", " ", "(", ")"))
    cleaned = cleaned.strip().replace(" ", "_")
    return cleaned[:100] or "unknown"


def parse_int_or_str(value: Any):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        v = value.strip()
        if v.lstrip("-").isdigit():
            return int(v)
        return v
    return value


def json_bytes(obj: dict) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


RETAIN_FILE_MAX_AGE_HOURS = int(os.getenv("RETAIN_FILE_MAX_AGE_HOURS", "168"))


def cleanup_retained_files(retain_dir: Path, logger):
    cutoff_seconds = RETAIN_FILE_MAX_AGE_HOURS * 3600
    now = time.time()
    if not retain_dir.exists():
        return
    for path in retain_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            age_seconds = now - path.stat().st_mtime
            if age_seconds <= cutoff_seconds:
                continue
            path.unlink(missing_ok=True)
            logger({
                "ts": tstamp(),
                "type": "cleanup",
                "op": "delete_retained_file",
                "file": str(path),
                "age_hours": round(age_seconds / 3600, 2),
            })
        except Exception as e:
            logger({
                "ts": tstamp(),
                "type": "warn",
                "op": "delete_retained_file",
                "file": str(path),
                "err": e.__class__.__name__,
                "msg": str(e),
            })


def cleanup_files(paths: List[str], source_kind: str, src_msg: Any, logger):
    for path in paths:
        try:
            p = Path(path)
            if p.exists():
                p.unlink(missing_ok=True)
                logger({
                    "ts": tstamp(),
                    "type": "cleanup",
                    "op": "delete_temp_file",
                    "file": str(p),
                    "src_msg": src_msg,
                    "source_kind": source_kind,
                })
        except Exception as e:
            logger({
                "ts": tstamp(),
                "type": "warn",
                "op": "delete_temp_file",
                "file": path,
                "src_msg": src_msg,
                "source_kind": source_kind,
                "err": e.__class__.__name__,
                "msg": str(e),
            })
