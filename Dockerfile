# -------- docker/Dockerfile --------
FROM python:3.10-slim

# 安裝必要系統套件（git 只在 build 時抓依賴時需要）
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git && \
    rm -rf /var/lib/apt/lists/*

# 建立非 root 帳戶
ARG USER=tguser
ARG UID=1000
RUN useradd -m -u ${UID} ${USER}

WORKDIR /app

# 先複製 requirements 以便分層快取
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 可調整 Telethon internal timeout，預設 10 秒
ARG TELETHON_NO_UPDATES_TIMEOUT=10
ENV TELETHON_NO_UPDATES_TIMEOUT=${TELETHON_NO_UPDATES_TIMEOUT}

# patch Telethon _updates/messagebox.py
RUN python - <<'PY'
from pathlib import Path
import telethon
import os

timeout = os.environ["TELETHON_NO_UPDATES_TIMEOUT"]

p = Path(telethon.__file__).resolve().parent / "_updates" / "messagebox.py"
text = p.read_text(encoding="utf-8")

old = "NO_UPDATES_TIMEOUT = 15 * 60"
new = f"NO_UPDATES_TIMEOUT = {timeout}"

if old not in text:
    raise SystemExit(f"pattern not found in {p}")

p.write_text(text.replace(old, new), encoding="utf-8")
print(f"patched {p}: {new}")
PY

# 驗證 patch 真係生效
RUN python - <<'PY'
import telethon._updates.messagebox as mb
print("VERIFY NO_UPDATES_TIMEOUT =", mb.NO_UPDATES_TIMEOUT)
PY

# 再複製程式碼
COPY main.py .
COPY tg_forwarder ./tg_forwarder

# 建立資料夾作 下載 / session 儲存
RUN mkdir -p /app/downloads \
    && chown -R ${USER}:${USER} /app

USER ${USER}

# 預設執行（runtime behavior controlled by APP_MODE env: listen | send）
CMD ["python", "-u", "main.py"]