# -------- docker/Dockerfile --------
FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git && \
    rm -rf /var/lib/apt/lists/*

ARG USER=tguser
ARG UID=1000
RUN useradd -m -u ${UID} ${USER}

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG TELETHON_NO_UPDATES_TIMEOUT=15
ENV TELETHON_NO_UPDATES_TIMEOUT=${TELETHON_NO_UPDATES_TIMEOUT}

RUN python - <<'PY'
from pathlib import Path
import re
import telethon
import os

timeout = os.environ["TELETHON_NO_UPDATES_TIMEOUT"]
p = Path(telethon.__file__).resolve().parent / "_updates" / "messagebox.py"
text = p.read_text(encoding="utf-8")

pattern = r"NO_UPDATES_TIMEOUT\s*=\s*\d+\s*\*\s*\d+"
replacement = f"NO_UPDATES_TIMEOUT = {timeout}"

new_text, n = re.subn(pattern, replacement, text, count=1)
if n != 1:
    raise SystemExit(f"Failed to patch NO_UPDATES_TIMEOUT in {p}")

p.write_text(new_text, encoding="utf-8")
print(f"Patched {p} -> NO_UPDATES_TIMEOUT = {timeout}")
PY

COPY main.py .
COPY tg_forwarder ./tg_forwarder

RUN mkdir -p /app/downloads \
    && chown -R ${USER}:${USER} /app

USER ${USER}

CMD ["python", "-u", "main.py"]