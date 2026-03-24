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
RUN python -c "from pathlib import Path; import re, telethon, os; \
timeout=os.environ['TELETHON_NO_UPDATES_TIMEOUT']; \
p=Path(telethon.__file__).resolve().parent / '_updates' / 'messagebox.py'; \
text=p.read_text(encoding='utf-8'); \
pattern=r'NO_UPDATES_TIMEOUT\\s*=\\s*\\d+\\s*\\*\\s*\\d+|NO_UPDATES_TIMEOUT\\s*=\\s*\\d+'; \
replacement=f'NO_UPDATES_TIMEOUT = {timeout}'; \
new_text,count=re.subn(pattern, replacement, text, count=1); \
assert count == 1, f'PATCH FAILED: NO_UPDATES_TIMEOUT not found in {p}'; \
p.write_text(new_text, encoding='utf-8'); \
verify=p.read_text(encoding='utf-8'); \
assert replacement in verify, f'PATCH VERIFY FAILED in file {p}'; \
print(f'patched {p}: {replacement}')"

# 驗證 import 後真係生效，唔啱就直接 fail build
RUN python -c "import os, telethon._updates.messagebox as mb; \
expected=int(os.environ['TELETHON_NO_UPDATES_TIMEOUT']); \
actual=mb.NO_UPDATES_TIMEOUT; \
print('VERIFY NO_UPDATES_TIMEOUT =', actual); \
assert actual == expected, f'VERIFY FAILED: expected {expected}, got {actual}'"

# 再複製程式碼
COPY main.py .
COPY tg_forwarder ./tg_forwarder

# 建立資料夾作 下載 / session 儲存
RUN mkdir -p /app/downloads \
    && chown -R ${USER}:${USER} /app

USER ${USER}

# 預設執行（runtime behavior controlled by APP_MODE env: listen | send）
CMD ["python", "-u", "main.py"]