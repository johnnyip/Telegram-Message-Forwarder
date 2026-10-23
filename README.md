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
Direct-forward Kafka jobs are therefore disabled by default; `ENABLE_DIRECT_FORWARD_JOBS=true` is an explicit override for non-default deployments that can actually consume those jobs.

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
- `KAFKA_PRODUCER_COMPRESSION_TYPE`
- `KAFKA_PRODUCER_LINGER_MS`
- `KAFKA_PRODUCER_MAX_BATCH_SIZE`
- `KAFKA_CONSUMER_COMMIT_EVERY`
- `KAFKA_CONSUMER_COMMIT_INTERVAL_SECONDS`
- `LARGE_MEDIA_FORWARD_THRESHOLD_MB`
- `FORWARD_POLICY`
- `FORWARDABLE_SOURCE_CHATS`
- `NONFORWARDABLE_SOURCE_CHATS`
- `FORUM_TOPIC_DELIVERY_MODE`
- `SKIP_MESSAGE_TYPES`
- `LOG_LEVEL`
- `ENABLE_DEBUG_LOGS`
- `TELETHON_DEBUG`
- `TZ`
- `DISPLAY_TIMEZONE`

### Listen-only
- `APP_MODE=listen`
- `API_ID`
- `API_HASH`
- `SESSION_NAME`
- `LISTEN_EDITED_MESSAGES`
- `ENABLE_DIRECT_FORWARD_JOBS`

### Send-only
- `APP_MODE=send`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_BOT_API_BASE`
- `TELEGRAM_BOT_API_FILE_BASE`
- `TELEGRAM_BOT_LOCAL_MODE`
- `TEXT_TARGET_PARALLEL`
- `MEDIA_TARGET_PARALLEL`
- `BOT_SEND_AS_DOCUMENT`
- `BOT_SEND_CONCURRENCY`
- `HEALTH_FILE`
- `STALE_FILE_HOURS`
- `BOT_UPLOAD_MAX_MB`
- `BOT_ALBUM_MAX_TOTAL_MB`

## Kafka write-reduction knobs

- Producer defaults now favor lower broker churn:
  - `KAFKA_PRODUCER_COMPRESSION_TYPE=gzip`
  - `KAFKA_PRODUCER_LINGER_MS=50`
  - `KAFKA_PRODUCER_MAX_BATCH_SIZE=131072`
- Send-mode consumers still use manual commits, but offsets are now batched by default:
  - `KAFKA_CONSUMER_COMMIT_EVERY=100`
  - `KAFKA_CONSUMER_COMMIT_INTERVAL_SECONDS=5`
- To disable producer compression explicitly, set:
  - `KAFKA_PRODUCER_COMPRESSION_TYPE=none`

## Selective skip mode

Listen mode can drop selected message types before they ever become Kafka jobs:

- `SKIP_MESSAGE_TYPES=text`
- `SKIP_MESSAGE_TYPES=image`
- `SKIP_MESSAGE_TYPES=text,image`

`image` applies to Telegram photos only. Videos, documents, audio, and other media continue normally.

## Forum topic delivery mode

When a target chat is a forum/topic-enabled Telegram group:

- `FORUM_TOPIC_DELIVERY_MODE=mirror`
  - current default
  - send to the sender's individual topic and also keep the general-copy behavior
- `FORUM_TOPIC_DELIVERY_MODE=topic_only`
  - if a sender-specific topic is available, send only to that topic
  - if no topic can be resolved/created, fall back to general delivery

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

## Bot upload limits

- Official cloud Bot API (`https://api.telegram.org`) now defaults to `BOT_UPLOAD_MAX_MB=50`
- This avoids pointless `sendVideo` / `sendDocument` retries that end in `413 Request Entity Too Large`
- If you run a local Bot API server, set:
  - `TELEGRAM_BOT_LOCAL_MODE=true`
  - optionally `TELEGRAM_BOT_API_BASE` / `TELEGRAM_BOT_API_FILE_BASE`
- Local-mode default upload threshold becomes `2000MB`
- You can still override either path explicitly with:
  - `BOT_UPLOAD_MAX_MB`
  - `BOT_ALBUM_MAX_TOTAL_MB`

## Reliability improvements

- **Mode-specific startup summary**
  - listen mode logs `sender=telethon-user producer=true consumers=false`
  - send mode logs `sender=telegram-bot producer=false consumers=true`
- **Startup banner**
  - listen mode prints `🚀 tg-forwarder listen up | session=<name> | receiver=telethon | kafka=producer`
  - send mode prints `🚀 tg-forwarder send up | sender=telegram-bot | kafka=consumers`
- **Bot auth probe on startup**
  - send mode verifies bot token with `get_me()`
- **Album fallback**
  - if `send_media_group` fails, send mode falls back to single-file sends
- **Rate-limit protection**
  - send mode uses a global bot send semaphore via `BOT_SEND_CONCURRENCY` (production default: `1`)
  - `RetryAfter` and timeout errors are retried with backoff before failing the Kafka job
- **Stale file warning**
  - send mode warns when local media files are older than `STALE_FILE_HOURS` (default 72)
- **Health file**
  - runtime writes a health marker file (default `/tmp/tg-forwarder-health.txt`)

## Forwarded message footer

Forwarded text and media captions append the source message time as the final line:

```text
Original send HH:MM
```

The displayed time is converted using:
1. `DISPLAY_TIMEZONE`
2. fallback `TZ`
3. fallback `Asia/Hong_Kong`

For Johnny's production deploy, this is intended to display **HKT**.

## Production defaults

Recommended production defaults for `APP_MODE=send`:

- `TEXT_TARGET_PARALLEL=false`
- `MEDIA_TARGET_PARALLEL=false`
- `BOT_SEND_CONCURRENCY=1`
- `BOT_STARTUP_SMOKE_TEST=false`
- `VERBOSE_JOB_LOGS=false`
- `ENABLE_DEBUG_LOGS=false`
- `TELETHON_DEBUG=false`

## Runtime and reproducible dependencies

Baseline checked 2026-10-23: **Python 3.14.9** (standard GIL build),
Telethon 1.45.0, cryptg 0.6.0, python-dotenv 1.2.4, aiokafka 0.14.0,
python-telegram-bot 22.8, redis[hiredis] 8.1.0 and Pillow 13.0.0.
Python 3.10 reached EOL on 2026-10-01. Python 3.15 is now stable, but this
bounded upgrade stays on the supported 3.14 maintenance line (EOL 2030-10):
cryptg and aiokafka currently publish CPython 3.14 wheels, not 3.15 wheels.
3.14.9 is the October 21 expedited security release; do not use 3.14.7 in production.

Sources: [Python lifecycle](https://devguide.python.org/versions/),
[3.14.9 release/security notes](https://www.python.org/downloads/release/python-3149/),
[official image tag](https://hub.docker.com/v2/repositories/library/python/tags/3.14.9-slim-bookworm).
Exact latest package releases, Requires-Python, release files and published advisories
were checked through `https://pypi.org/pypi/<package>/json` (substitute the package names above).
No direct-package advisories were listed there at the time; that is not a complete security audit.

Keep direct pins in `requirements.in` and the complete transitive hash lock in
`requirements.txt`. Use Python 3.14 when regenerating; other Python minors can select
different environment markers. Existing requirements.txt-based installs remain supported:

~~~bash
python3.14 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip check
# Only when intentionally refreshing the lock:
.venv/bin/python -m pip install pip-tools==7.6.2
.venv/bin/pip-compile --generate-hashes --strip-extras --no-emit-index-url --no-emit-trusted-host --output-file requirements.txt requirements.in
~~~

Docker installs with hash checking and runs the offline tests during the existing Jenkins
Build and Offline Tests stage, before publishing. Jenkins defaults both
`PUBLISH_IMAGES=false` and `PROMOTE_LATEST=false`. A missing legacy persisted
`PROMOTE_LATEST` parameter is treated as false; only an explicit true value together with
`PUBLISH_IMAGES=true` can reach the separate `latest` promotion stage. Publication pushes
a unique `sha-<full-source-sha>-build-<build-number>` source tag first. The image carries
full-SHA revision, source URL, and branch labels. Jenkins records the pulled image config
ID, repository digest, labels, and source SHA in an archived receipt, then writes a
full-SHA completion marker only after all requested publication steps succeed. A bounded
push's nonzero/timeout status remains a failed build, not a successful receipt. Deploy by
the recorded digest; tags are not registry-enforced immutable. Tests run with
`--network none`, no runtime mounts, and an overridden entrypoint. Review existing Jenkins
job parameter values before a build: changing defaults does not override saved values, and
an old saved `PUBLISH_IMAGES=true` can still publish the unique SHA tag but cannot advance
`latest` without the new, explicitly enabled promotion parameter. Credentials use the existing
Jenkins binding, stdin login, and a temporary isolated Docker config; they are not written
to receipts. The Telethon internal timeout patch still asserts its
match and imported value, so upstream internal changes fail the build instead of silently
losing the patch. The image tag and OS packages are not digest/snapshot locked; dependency
hash locking does not make the whole image bit-for-bit reproducible. pyaes is distributed
as an sdist; its hash is locked, but its isolated build tooling is not.

Offline tests cover text-job serialization through a fake producer into the real sender
wrapper/fake Bot API, media snapshot-job handoff, missing files/all-target failures,
bounded Kafka/Bot retries (including PTB timedelta RetryAfter), and dedup/journal TTLs.
They do not prove live Telegram download, Kafka offset/rebalance behavior or delivery.
No application entrypoint, real sessions/tokens or live broker are required for tests.

Upgrade validation was run locally on Python 3.14.7 with the new locked packages; the exact
3.14.9 container build/test remains a release gate (no Docker access during implementation).
Before rollout, build/test the pinned image and verify dependency health/security scanning.
Use a bounded staging route and fresh explicitly authorized source media, then correlate
listener snapshot, Kafka job and intended destination IDs. Do not replay arbitrary live jobs.
Preserve sessions, shared spool, send journal, Redis data and Kafka group/offsets. Stop the old
sender before replacing it; never run competing senders as an upgrade test. Keep the prior
immutable image/config available for rollback without resetting offsets or deleting state.
Existing delivery is not exactly-once: partial-target failures, ambiguous API timeouts,
processing-journal crash windows, and file-journal concurrent writers remain hazards.
A dependency upgrade does not resolve those pre-existing semantics.

## Docker image
The Docker image includes:
- `main.py`
- `tg_forwarder/`
- Python dependencies from `requirements.txt`

Rebuild the image after changing dependencies:

```bash
docker build -t johnnyip/telegram-message-forwarder:latest .
```
