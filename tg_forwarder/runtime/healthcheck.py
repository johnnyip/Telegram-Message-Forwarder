import os
from pathlib import Path


def write_health_file(path: str, content: str = "ok"):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


def default_health_path() -> str:
    return os.getenv("HEALTH_FILE", "/tmp/tg-forwarder-health.txt")


__all__ = [
    "default_health_path",
    "write_health_file",
]
