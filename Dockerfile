# -------- docker/Dockerfile --------
FROM python:3.10-slim

# 安裝必要系統套件（git 只在 build 時抓依賴時需要）
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git && \
    rm -rf /var/lib/apt/lists/*

# 建立非 root 帳戶 (可選，增加安全)
ARG USER=tguser
ARG UID=1000
RUN useradd -m -u ${UID} ${USER}

WORKDIR /app

# 先複製 requirements 以便分層快取
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 再複製程式碼
COPY main.py .

# 建立資料夾作 下載 / session 儲存
RUN mkdir -p /app/downloads \
    && chown -R ${USER}:${USER} /app

USER ${USER}

# 預設執行
CMD ["python", "-u", "main.py"]