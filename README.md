# Telegram_Message_Forwarder
 Telegram Message Forwarder

`docker-compose.yml`
```
version: '3'
services:
  forwarder:
    image: johnnyip/telegram-message-forwarder:latest
    environment:
      - CHAT_IDS=-10018065..., -100192221...
      - CHAT_TARGET=username1, username2...
      - DELAY_SECONDS=5
      - API_ID=
      - API_HASH=
    volumes:
      - ./downloads:/app/downloads
      - ./my_account.session:/app/my_account.session
    restart: "unless-stopped"
```
