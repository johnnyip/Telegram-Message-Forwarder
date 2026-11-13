# Telegram media workbench (opt-in control runtime)

## Isolation and identities

Set **APP_MODE=control** to early-dispatch before legacy main.py environment capture,
route validation, clients, spool cleanup or Kafka. listen/send/migrate/reconcile are
unchanged. The controller uses the downloader bot via TELEGRAM_BOT_TOKEN; downloads
use a **separately authorized Telethon USER session**. Never use the listener live
session or start two pollers for the downloader bot. Stop the old downloader first.
Startup neither logs credentials nor initiates interactive login. The downloader
account get_me ID is NOT a controller allowlist or owner ID.

CONTROL_ENV_FILE loads a protected dotenv file only in control mode, before API,
session or config capture. Explicit environment overrides the file. Legacy .env is
not implicitly loaded here. Unreadable files fail startup without printing values.
Keep secrets outside Git and Portainer stack environment; bind the file read-only
and make it readable by the actual container UID, not by all users.

## Environment

- CONTROL_ALLOWED_USER_IDS: comma-separated positive decimal user IDs. Empty denies
  everyone. Malformed entries, trailing commas, zero, signs and oversized IDs fail
  startup. Every handler validates actual from_user.id. Bots and sender_chat/anonymous
  channel identities are denied. This does not gate existing route monitoring.
- CONTROL_TARGETS_JSON: explicit JSON list of objects with numeric chat_id and an
  optional positive thread_id. Default empty: download-only. No free-form destination
  input. The downloader bot must actually access each destination; memberships of
  other bots do not imply access. Initial known target is owner private 5268216341.
- API_ID / API_HASH: downloader user API application credentials.
- MEDIA_USER_SESSION_STRING: separate Telethon StringSession; preferred. OR explicitly
  set MEDIA_USER_SESSION_FILE to an existing dedicated Telethon .session file.
  Exactly one is required. SESSION_NAME is never used as a fallback. Do not point
  at the listener session or a concurrently running Pyrogram session.
- TELEGRAM_BOT_TOKEN: existing downloader bot identity, never the pipeline bot by
  inference. Bot does control polling AND on-demand archive uploads.
- CONTROL_EXPECTED_ARCHIVE_MOUNT: optional exact Linux mountpoint; set /media for
  the NUC deployment. CONTROL_EXPECTED_ARCHIVE_FSTYPES=nfs,nfs4 requires actual
  mountinfo filesystem type, not merely a Docker bind mount. Missing/wrong mounts
  fail closed before mkdir/auth; archive operations recheck before writing.
- CONTROL_STATE_DIR: default /data/control. SQLite, WAL and lock stay on NUC-local
  storage, **not NFS**. Startup/Store explicitly reject NFS, CIFS, SSHFS and other
  network backing filesystems using Linux mountinfo, even through bind mounts. CONTROL_DB optionally overrides the exact database path.
- MEDIA_ARCHIVE_DIR: default /data/media-archive; MEDIA_ARCHIVE_ROOT is an alias.
  May use the existing NFS video mount. Keep separate from DOWNLOAD_DIR and spool.
- CONTROL_MAX_ITEMS=1000; CONTROL_MAX_BYTES=21474836480; CONTROL_RESERVE_BYTES=1073741824.
- CONTROL_CONFIRM_ITEMS=100: confirmation at/above this many items. Unknown size or
  partial snapshot also always requires confirmation. Limits still apply afterward.
- CONTROL_CONCURRENCY=1 (only 1 or 2 accepted), CONTROL_RETRIES=3,
  CONTROL_SCAN_LIMIT=10000, CONTROL_WATCH_INTERVAL=300 (seconds).
  CONTROL_WATCH_SETTLE_SECONDS=30 holds an arriving trailing album until its member
  signature is stable across observations; older closed albums can commit first.
- CONTROL_BOT_UPLOAD_MAX_BYTES=51380224: conservative per-file bot upload cap.
  Photo upload additionally caps at 10 MiB. Bigger media remains archived and reports
  explicit failure; no native-forward fallback. This setting does not configure a
  local Bot API server. Bot API photo dimension/ratio limits may also reject uploads.

## UI and security

Send a public https t.me username/post URL or private /c/channel/post URL, optionally
with comment=ID or single. Arbitrary hosts, ports, credentials, extra query options,
fragments and oversized IDs are rejected. Forwarded channel metadata is accepted;
hidden-origin forwards require the original URL, not a misleading forwarded copy.

Preview first replies immediately, then edits that same reply asynchronously via a
persistent preview queue. Only distinct nonempty media scopes appear: single,
selected album, discussion, combined. Comment URLs select that comment/album;
discussion/combined still refer to the parent post. Photo/video/all filters rebuild
scopes. Download buttons are primary; action menus offer configured destination/topic
forwarding or download+forward. IDs in buttons are opaque, persisted, expiring and
bound to owner, chat and preview-message ID. Repeated actions atomically reuse tasks.

/health is a real authenticated handler smoke check, with no download or forwarding.
/tasks lists the owner's tasks in this chat; /task ID gives details; /cancel ID,
/retry ID (retry failed), /resume ID (cancelled or failed) preserve prior successes.
Task cards retain owner/chat/reply-bound Details, Cancel, Retry failed or Resume buttons
as applicable. /tasks renders at most 20 owner/chat tasks with bound inline controls.
Progress shows completed/total and downloaded/total separately, plus terminal remaining
counts; download completion never implies forward completion. Action, watch and large
confirmation menus provide Back navigation without changing the stored snapshot.
Progress is throttled to at most once per 10 seconds plus terminal updates. FloodWait
and delayed states are visible. Cancel cooperates at file/progress and send boundaries;
it cannot undo a request already accepted by Telegram. Ambiguous sends are never
blindly retried: inspect destination and operation records before operator repair.

## Source resolution and archives

Source resolution walks actual grouped_id boundaries in both directions, preserving
source order, not an arbitrary +/-10 ID window. GetDiscussionMessage can identify the
last root-album member; the resolver expands the root album and uses its first ID for
GetReplies pagination. Auto-forwarded root duplicates are excluded. GetReplies scopes
nested replies to the thread; the code never scans the entire discussion group.
Permission/deletion/scan failures are partial or unavailable, not successful zeroes.
Private channel access hashes are retained with snapshots for task restart hydration.

Fixture evidence: channel -1004433596337 post 4833..4842 has 10 photos; group
-1003837422582 root 24124..24133, 62 comment messages (49 photos,11 videos,2 text),
six 10-item albums; comment24204 selects the last 6-photo/4-video album. Combined
70 media =59 photos+11 videos. This is an offline fixture, not a live acceptance claim.

Permanent archives now follow the original tangyoha downloader default workflow:

    TG_Download/<chat.title or configured ID>/<message YYYY_MM>/<message ID> - <original stem><extension>

Missing document filenames become ID.ext, without a repeated ID stem. Photos use
Pyrogram DOCUMENT-type unique IDs (not raw media IDs). Unicode/spaces/emoji and original
suffixes remain. Discussion items use their actual source chat/message metadata.
CONTROL_ARCHIVE_TIMEZONE=UTC is an explicit **provisional** default; original NAS
runtime timezone is not verified. Confirm/set it before migration, especially at month
boundaries. Source metadata is captured with snapshots and exposed in the existing
operator --inspect response as archive_metadata; no cloned session is required.

The thin Telethon adapter follows original metadata -> finalized-file check -> owned
temporary download -> completion/size validation -> durable finalization -> success.
A valid owned original-format file is reused without another download even if its
SQLite cache row is absent. Unlike upstream's overwrite-capable move, collisions and
symlinks fail closed. Manifests remain private in .metadata; no visible custom post/album
folders are created. Old persisted snapshots remain readable. Changing archive root
requires explicit cache-path migration, not redownloading existing files. See
[original-downloader-storage.md](original-downloader-storage.md) for pinned source,
MIT attribution, supported defaults, migration plan and remaining deployment inputs.
Highest available Telegram quality is downloaded, not a claim of camera originals.

Temp files are exclusively created; publication is atomic no-clobber hard-link then
unlink of that owned temp (same-directory), so existing/orphan files cannot be silently
overwritten. Disk/byte limits are checked while streaming. Root/ancestor/component
symlinks and traversal are rejected. Archive/state directories must be operator-owned,
not writable by untrusted users (no claim of protection against a malicious same-UID
filesystem race). Crash-orphan files are retained for explicit operator reconciliation.
Nothing in forwarding spool cleanup references this archive root.

Cache identity is source chat/message/media revision; task identity includes owner+chat, selected IDs/media revisions,
action and destination/topic. Download success is not forward success. Destination A
is not B. Forward completion is per media+target, so overlapping single/album actions
do not resend completed items. Missing members can become a smaller album after prior
partial completion. Documents/audio/stickers and text-only replies are not downloadable
workbench scopes; text replies still remain in the source snapshot.

Bot uploads preserve album order and captions within Bot API limits. Captions longer
than 1024 characters fail explicitly rather than silently truncate; full captions remain
in manifests. Protected/noforwards sources reject on-demand forwarding, including
rechecks before upload; no native-forward or alternate-account bypass is attempted.
Downloaded files survive upload failures. Forward-only actions also populate durable
cache because the bot uploads files; they are not native Telegram forwarded messages.

## Watch new comments

Default OFF. Explicit action-menu confirmation enables only that post's future comments
with photo/video filter and download or download+forward destination/topic. /watches
lists owner/chat watches; /unwatch ID stops one. Re-subscribing explicitly reactivates it.
Canonical parent/thread references do not depend on the original selected comment
still existing. Cursor and persisted album-settling signatures survive restart. Cursor
never advances on partial scans, permission failures, size
confirmation requirements or task creation failure. Task insertion and cursor advancement commit in one SQLite transaction, revalidating
active state, generation and prior cursor after awaited API calls; unsubscribe or
re-subscribe invalidates in-flight observations and prevents their task insertion. Unknown/oversized batches wait for manual
selection rather than bypass confirmation. Polling uses cursor-bounded GetReplies min_id pagination on that exact persisted
thread, not full historical rescans. An exactly-full scan cap is not itself partial;
an additional item/boundary probe distinguishes complete and truncated results.
There is no autonomous channel monitoring. The legacy route option
include_discussion_media is separately opt-in/default-off; see media-shared.md.
It snapshots a qualified post once, not a future-comment watch.

The local rate gate persists delays for previews, watches, downloads and uploads.
Optional shared mode also coordinates admission through Redis (details below).
A separate session can still belong to the same Telegram account: use the same
MEDIA_ACCOUNT_RATE_KEY across participating processes; other clients are not gated.

## Read-only inspection in an existing deployed service

Run the following with the same nonsecret settings and CONTROL_ENV_FILE already set:

    python -m tg_forwarder.control.runtime --inspect https://t.me/TGFanChabiao/4833
    python -m tg_forwarder.control.runtime --inspect 'https://t.me/TGFanChabiao/4833?comment=24204'

When the service runs, CLI connects to its mode0600 local Unix inspection socket,
using the existing client, not a second auth owner. It only resolves/counts: no bot
polling, archive download or forwarding. This is an operator read-only IPC, not a
substitute for from_user authorization. Without an active socket, standalone inspect
acquires the exclusive state lock and performs only source reads. A stale socket after
an unclean death is safely replaced by the next exclusive state owner. No TCP port. The same restricted socket accepts exactly a health=true request and
returns a small status=ok response without any Telegram calls.

    python -m tg_forwarder.control.runtime --health

The health CLI only uses this socket; it never creates a Telethon client, bot poller,
database or source snapshot. Filesystem guards are checked locally as part of health.

## Example NUC Compose (documentation only; no deployment performed)

Pin IMAGE to the exact source-labelled release produced by existing authorized CI.
Keep the task database on a local NUC directory. Existing video NFS mount must be
verified before startup; do not silently download onto an unmounted local directory.

    services:
      media-control:
        image: johnnyip/telegram-message-forwarder:REPLACE_WITH_VERIFIED_RELEASE
        restart: unless-stopped
        environment:
          APP_MODE: control
          CONTROL_ENV_FILE: /run/secrets/control.env
          CONTROL_STATE_DIR: /data/control
          MEDIA_ARCHIVE_DIR: /media/TG_Download/workbench
          CONTROL_ALLOWED_USER_IDS: '5268216341'
          CONTROL_TARGETS_JSON: '[{"chat_id":5268216341}]'
          CONTROL_CONCURRENCY: '1'
          CONTROL_EXPECTED_ARCHIVE_MOUNT: /media
          CONTROL_EXPECTED_ARCHIVE_FSTYPES: nfs,nfs4
        read_only: true
        user: '1000:1000'
        tmpfs:
          - /tmp:size=64m,mode=1777
        healthcheck:
          test: [CMD, python, -m, tg_forwarder.control.runtime, --health]
          interval: 30s
          timeout: 10s
          retries: 3
        volumes:
          - /home/johnny/docker/tg-media-workbench/control.env:/run/secrets/control.env:ro
          - /home/johnny/docker/tg-media-workbench/state:/data/control
          - /volume1/video:/media
        # No Kafka, listener-session mount, route edits or image builds here.

The protected control.env contains API_ID, API_HASH, TELEGRAM_BOT_TOKEN and
MEDIA_USER_SESSION_STRING only as required. No values belong in Git or logs.

## Native/offline and deployed acceptance checklist

Native (existing GB10 venv, no Docker, Telegram or broker calls):

    /home/johnny/Developer/mac-dev-20261028/Telegram-Message-Forwarder/.venv/bin/python -m unittest discover -s tests -v
    /home/johnny/Developer/mac-dev-20261028/Telegram-Message-Forwarder/.venv/bin/python -m compileall -q main.py tg_forwarder/control
    git diff --check

After parent-owned CI/publish/real NUC deployment:
1. Stop old bot poller, verify separate downloader auth, local SQLite path, actual
   archive NFS mount/UID permissions, source-labelled image, no logged secrets.
2. Read-only --inspect both URLs: main album10, discussion60 media/62 messages,
   all70/59 photos/11 videos; comment24204 single1 and selected album10. Current source
   may change; explain partial/deleted evidence rather than hardcode expected counts.
3. Owner sends /health to downloader bot through Telegram. Unknown user /tasks and
   links must not resolve or disclose data; copied/expired/cross-chat callbacks deny.
   Do not fabricate production updates or bypass authorization for smoke testing.
4. Owner selects one photo download, checks archive/manifest; then selected album,
   full discussion and combined within approved scope. No duplicated root album or
   extra media; verify bytes/quality locally against Telegram representation.
5. Authorized owner-private forwarding and download+forward: actual album groups,
   captions, topic if configured. Repeated click and overlapping scope do not resend;
   download-only completion does not suppress a later first forward.
6. Cancel/retry/restart a bounded task; demonstrate persisted offset and callbacks,
   failed-item-only reuse, visible delayed rate gate and conservative ambiguous state.
7. Explicitly opt in a post watch, observe only new comments, restart and unsubscribe.
   Validate no watch before opt-in and no archive/destination deletion on source delete.
8. Existing listen/send pipeline still processes a fresh authorized route event and
   preserves its original caption/edit behavior. Offline tests are not deployed proof.

## Shared source bytes and conservative delivery bridge

MEDIA_SHARED_ENABLED=false preserves standalone behavior. When explicitly enabled,
control creates configured_bridge only after CONTROL_ENV_FILE loading. See
[media-shared.md](media-shared.md) for required Redis/shared-root/account-label settings,
NAS path mappings, retention, ACLs and the default-off route discussion snapshot.
Both processes must point MEDIA_SHARED_ROOT at the same physical operator-owned NAS
cache, using matching REDIS_PREFIX and MEDIA_ACCOUNT_RATE_KEY when using one user account.
Never use credentials as identity labels. Control derives its delivery principal from
actual bot.get_me().id, never from a guessed username or token text.

Control now publishes a **v2 metadata reference** to its one permanent original-format
file, not a second hashed payload. Configure permitted root labels consistently:

    MEDIA_SHARED_ARCHIVE_ROOTS_JSON={"control":"/media/TG_Download"}

Control registers its trusted MEDIA_ARCHIVE_DIR as control and refuses a conflicting
mapping. Each legacy process maps the same label to its mount of that same physical
archive. Redis cannot supply arbitrary absolute paths. Lookup checks owned regular
files, traversal/symlinks, size and SHA256 off the event loop. Missing references miss;
corrupt or unknown-root references fail closed. Canonical lifetime belongs to the
operator, never cache cleanup. V2 references have no TTL and no 2 GiB blob size limit;
CONTROL_MAX_BYTES still bounds each task. Shared index errors preserve completed files.

V1 hashed cache objects remain readable/migratable. Configured bridges now default
MEDIA_SHARED_PERSISTENT_CACHE_ENABLED=false; set this explicitly false in NAS and NUC.
Legacy fresh downloads remain transient spools: upload and existing cleanup proceed
unchanged, with no new persistent v1 blob and no transient-path index reference.
Control-first canonical files remain reusable by legacy; legacy-only bytes are not
promised to survive spool cleanup for a future explicit control download. Existing old
objects require explicit migration/retirement. Manual gc_owned is optional maintenance,
not a promise of automatic cleanup. No GC daemon or new scheduler is introduced.

The composite rate gate retains SQLite rate_until and visible delayed/restart task
states, and joins shared Redis FloodWait writes before admission. Shared mode can
coordinate processes using the same account label; disabled mode is only process-local.
A delay cannot revoke in-flight RPCs. Telegram may perform internal download chunk RPCs;
this gate is admission coordination, not a strict bandwidth/per-packet limiter.

Local per-item forward completion remains authoritative for overlapping scopes.
With shared mode enabled its identity additionally distinguishes bot, caption and media
kind. Before sending a pending album/single, control claims a shared operation including
source identities, numeric destination/topic, verified bot principal, action, exact
ordered captions and media kinds. Different groupings/bots/topics/legacy mirror semantics
are intentionally not equivalent. A shared successful receipt restores message IDs and
marks local pending operations done without another send. Receipt length/IDs must match
the requested pending group; incomplete receipts are ambiguous, never completed.

Known Telegram rejection releases only the owning shared claim and marks local items
failed; local Bot API size/caption bounds reject before the external effect. Cancellation,
transport uncertainty, incomplete result or an existing unsettled shared claim leaves
local ambiguity and a durable shared claim, never an automatic resend. Operators must
reconcile actual destination evidence, not delete claims merely because time elapsed.
Redis persistence/eviction policy is part of this guarantee; lost Redis evidence cannot
be reconstructed as exactly-once proof. Use a dedicated local CONTROL_STATE_DIR per bot;
changing bot identity while reusing completed task rows is not a migration mechanism.

## Independent review hardening

Preview notification/edit failures are isolated from all workers. Missing/deleted reply
messages fail that preview, not the service. RetryAfter is bounded/persisted and retries
reuse the stored preview snapshot rather than resolve a new source snapshot. Forbidden
ingress updates advance the durable getUpdates offset without storing raw payloads;
handler authorization remains defense in depth. Removed owners cannot run persisted
previews, watches or tasks.

Download completion requires a non-None result, nonempty file and agreement with known
snapshot/current Telegram byte size or progress total. None/empty/truncated results
never populate cache or publish media; owned temporary files are cleaned.

A read-only container root is compatible with StringSession: all persistent writes are
in CONTROL_STATE_DIR and MEDIA_ARCHIVE_DIR, plus /tmp tmpfs. Parent must verify the
container UID can read the protected secret file and write both local state and the
NAS-mapped archive owner; filesystem type checks do not grant permissions.

## Send already-downloaded photos into this private chat

Completed download/both task cards with valid archived photos offer
**📤 傳送已下載圖片到此對話（N）**. Only photos are included; video-only tasks
have no button. Clicking creates one durable cache-only photo-send task addressed
only to the originating private owner chat, which must also be explicitly present
in CONTROL_TARGETS_JSON as that numeric chat with no topic. The deployed owner is
@lasscc, numeric UID 5268216341; authorization remains configuration-based, not a
hardcoded username. This button grants no group or topic access.

**Existing completed tasks work without another download:** send /tasks, click that
task's 詳情 button (or open /tasks TASK_ID). The refreshed card renders the new button
from its persisted task and cache records. Older completion messages are not mass
edited on startup. Expired controls require a fresh /tasks card. Nothing is sent
until the owner explicitly clicks the photo-send button.

The worker preflights every selected local file before sending. Cache misses, unsafe
paths, missing/empty files or known size mismatches fail clearly: there are no source
entity/message lookups, downloads or shared-cache materialization fallbacks, including
after restart. Archive files are never removed by this action. Existing Bot API upload,
caption, job-size, rate and protection-snapshot constraints still apply; it deliberately
does not query current Telegram source state because the action is cache-only.

Original source albums remain grouped by source and split into at most ten photos per
send; singles remain singles. The origin task ID plus cached_photos action identifies
one durable explicit resend, separate from normal forward/both task receipts. Repeated
clicks reuse that task rather than trigger another copy. Per-item operations and shared
receipts use their own cached-photo namespace, also scoped to the origin task; normal
forward identities are unchanged. A failed definite rejection can use the task's existing
Retry control. Unknown/partial sends remain ambiguous and blocked pending operator
reconciliation, not automatically resent. This is one resend per original task, not an
unlimited resend button.
