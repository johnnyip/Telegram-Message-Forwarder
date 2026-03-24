# Telegram Message Forwarder

Telegram forwarder with split runtime modes:

- **APP_MODE=listen**
  - Uses **Telethon user session** (`API_ID` + `API_HASH` + `SESSION_NAME`)
  - Listens for incoming Telegram updates
  - Records lag / observability logs
  - Matches routes, gathers albums, applies ignore rules
  - Downloads media to local spool when needed
  - Publishes `text`, `media_file`, and `media_album_file` jobs to Kafka
  - Does **not** consume Kafka

- **APP_MODE=send**
  - Uses **Telegram Bot API** (`TELEGRAM_BOT_TOKEN`)
  - Consumes Kafka jobs
  - Sends text / media / albums to target chats from local downloaded files
  - Does **not** listen to source Telegram updates

## Design notes

### Why split listen and send?
Incoming delay investigation showed the most suspicious area is the **receive/update path**, not app-side preprocessing. Splitting the runtime lets the listen side stay light and keeps outbound send workload away from the incoming path.

### Media forwarding policy in split mode
In the current split architecture, the listen side prefers turning media into local snapshot-backed jobs for the send side:

- `text`
- `media_file`
- `media_album_file`

This is the safest design when send mode uses a bot token and should not depend on source-side Telethon session access.

## Important environment variables

### Shared
- `DOWNLOAD_DIR`
- `DELAY_SECONDS`
- `MEDIA_DELAY_SECONDS`
- `DOWNLOAD_CONCURRENCY`
- `DELETE_AFTER_SEND`
- `ALBUM_GATHER_SECONDS`
- `IGNORE_USERS`
- `IGNORE_IDS`
- `ROUTES_JSON`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TEXT_TOPIC`
- `KAFKA_MEDIA_TOPIC`
- `KAFKA_CONSUMER_GROUP`
- `LARGE_MEDIA_FORWARD_THRESHOLD_MB`
- `FORWARD_POLICY`
- `FORWARDABLE_SOURCE_CHATS`
- `NONFORWARDABLE_SOURCE_CHATS`
- `LOG_LEVEL`
- `ENABLE_DEBUG_LOGS`
- `TELETHON_DEBUG`

### Listen-only
- `APP_MODE=listen`
- `API_ID`
- `API_HASH`
- `SESSION_NAME`
- `LISTEN_EDITED_MESSAGES`

### Send-only
- `APP_MODE=send`
- `TELEGRAM_BOT_TOKEN`
- `TEXT_TARGET_PARALLEL`
- `MEDIA_TARGET_PARALLEL`
- `BOT_SEND_AS_DOCUMENT`
- `HEALTH_FILE`
- `STALE_FILE_HOURS`

## Bot media policy
Default behavior is to **keep the original format as much as possible**:

- original `photo` → send as photo
- original `video` / `animation` / `video_note` → send as video
- original `audio` / `voice` → send as audio
- original `document` → send as document

Optional override:

- `BOT_SEND_AS_DOCUMENT=true`
  - forces most non-audio media to be sent as document
  - useful only when you explicitly want to avoid Telegram media compression / preview behavior

## Reliability improvements

- **Mode-specific startup summary**
  - listen mode logs `sender=telethon-user producer=true consumers=false`
  - send mode logs `sender=telegram-bot producer=false consumers=true`
- **Bot auth probe on startup**
  - send mode verifies bot token with `get_me()`
- **Album fallback**
  - if `send_media_group` fails, send mode falls back to single-file sends
- **Stale file warning**
  - send mode warns when local media files are older than `STALE_FILE_HOURS` (default 72)
- **Health file**
  - runtime writes a health marker file (default `/tmp/tg-forwarder-health.txt`)

## Docker image
The Docker image includes:
- `main.py`
- `tg_forwarder/`
- Python dependencies from `requirements.txt`

Rebuild the image after changing dependencies:

```bash
docker build -t johnnyip/telegram-message-forwarder:latest .
```
