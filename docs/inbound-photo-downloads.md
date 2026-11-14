# Automatic received-photo downloads

Owner 5268216341 can upload/forward photos to the bot's private conversation.
Each received message automatically creates one durable download job (the original
downloader's limit=1 behavior). Ten photos in one album create ten jobs; there is
no additional confirmation, album-settling framework or album suppression flag.
Actual media_group_id is retained; each late photo is another independently
idempotent message. A caption URL, user/hidden/channel origin or lack of original
origin does not redirect the received photo into the original-post resolver.
Text URLs retain the existing original-post/thread workflow.

Only the real allowed, non-bot message actor in that owner's private chat can
queue these jobs. Forwarded sender identity is not authorization. The transport
is the existing initialized PTB bot's get_file/download_to_memory into an exclusive
task-owned temporary file, then the existing length/fsync/no-clobber/cache/manifest
lifecycle. Bot API file_id never enters manifests, logs, Redis identities or UI.
No new auth client, source RPC, automatic forwarding or persistent blob copy is
created. The existing explicit cached-photo-send button remains separately allowed.

## Limits and naming

This first path supports photos only, with the cloud Bot API getFile 20 MB (20,000,000 bytes) bound,
not Pyrogram MTProto's larger video/document downloads. Known over-limit files are
rejected before retrieval, actual getFile size and bytes written are also checked.
PTB buffers a cloud file response before writing it; server cloud limits and the
local byte guard are both relevant. Unknown size is verified after download; a
partial or empty file never becomes a canonical success. Retry refreshes getFile
rather than retaining an expiring file URL. Only the received Telegram photo
variant (largest pixel dimensions) is promised, not a camera original.

Default layout uses received owner conversation ID / received date's month in
CONTROL_ARCHIVE_TIMEZONE / received BOT message ID - original unique photo stem.jpg.
The independent bounded metadata codec reads photo ID from known v2/v4 Telegram
file-ID layouts. Legacy layouts were cross-checked against Pyrogram v2.0.106
file_id.py FileId.decode (LGPL-3.0-or-later upstream). Modern v4 minor >=32 compact
Thumbnail layouts are cross-checked against TDLib commit
c8828d50f5eac2fc7089ce1089d7ce9095c61b5e (Boost Software License 1.0):
- td/telegram/Version.h:48: RemovePhotoVolumeAndLocalId is version 32;
- td/telegram/files/FileLocation.hpp:51-64: modern photo stores id/hash then source,
  with no legacy volume/local IDs;
- td/telegram/PhotoSizeSource.hpp:35-44: Thumbnail contains file type and size;
- td/telegram/PhotoSizeSource.h:22-32: Thumbnail enum is 1.
No upstream source code copied or runtime dependency added. The existing DOCUMENT-type photo_unique_id formatter
produces the original downloader stem, e.g. photo ID 6154650511710734502 ->
AgADpsgxGwi2aVU. A Bot API PhotoSize.file_unique_id is NOT assumed equivalent.
Unknown formats have explicit naming_codec=stable-fallback-v1 and a stable hashed
bot-photo-...jpg stem; they are not represented as exact-original names. The task
keeps its true bot principal and file_unique_id in a nonnumeric bot-inbound revision
namespace, never a fabricated original SourceRef or numeric MTProto revision.
The archive timezone remains operator-configured; UTC is only the current default.

## Explicit local recovery of retained done arrivals

Run only in the same existing control-service container namespace, after stopping
its poller/workers. Parent/operator owns all production actions. This CLI only
validates and enqueues; it never downloads, initializes a Bot, contacts Redis,
mutates raw updates/status/history/offsets or creates fake callbacks/updates.
It takes the SAME <CONTROL_DB>.lock as normal runtime. On restart the normal worker
consumes the normal durable jobs. Repeating a plan returns the same jobs.

Plan schema (private local JSON, mode 600):

    {
      "version": 1,
      "owner_uid": 5268216341,
      "bot_id": 6317800517,
      "db": "/exact/container/path/control.sqlite3",
      "updates": [
        {"update_id": 356787102, "payload_sha256": "<sha256 of exact stored UTF-8 payload column>"}
      ]
    }

For the independently inventoried real batch, list ONLY update IDs
356787102 through 356787111 inclusive, each with its exact stored payload hash.
Their received bot message IDs are 73263..73272, one media_group_id, total 1,111,651
bytes / maximum 169,619 (parent's read-only inventory). Do not generate fictional
file IDs or reset those done rows. A plan hash binds every current payload field,
including its actual message/file IDs. Batch bound 1..32; done status, owner,
private chat, photo data and current bot principal are revalidated. Keep the plan
private even though no bot token/file_id is in its schema.

Dry run (explicit PUBLIC config in environment; no CONTROL_ENV_FILE read):

    CONTROL_ALLOWED_USER_IDS=5268216341 CONTROL_DB=/exact/container/path/control.sqlite3 \
      python -m tg_forwarder.control.recover_inbound --plan /private/recovery.json

Apply loads existing CONTROL_ENV_FILE with runtime override=False, matches the
plan bot ID to the configured token's numeric bot principal and any stored
bot_principal, then acquires the existing state flock and atomically inserts all
jobs or none. No getMe/network call or second bot session is needed:

    python -m tg_forwarder.control.recover_inbound --plan /private/recovery.json --apply --quiescent

Start the normal service afterwards. Verify ten corresponding jobs complete and
canonical files/manifest/cache references exist; do not infer download completion
from the original updates' done state. Exact-original filename compatibility for
the retained real file IDs can be verified offline by the dry-run codec counts;
unknown formats remain explicitly labelled fallback rather than guessed originals.

## Separate storage compatibility settings

NUC: MEDIA_ARCHIVE_FILE_OWNER_UIDS=0,1024 explicitly admits the known original
root-owned files and NFS server-mapped file owner in addition to the process and canonical-root owners. Defaults remain
unchanged. Strict bounded comma-separated numeric UIDs only; no ownership probes,
parent-owner inference, chmod/chown/ACL changes or blanket foreign UID trust.
Secure canonical files still must be nonempty regular files, nonsymlink and not
group/other writable. The shared reference validator and migration use the same
policy, including explicitly planned empty-directory retirement. Never set this
allowlist merely because an unknown file fails validation.

NAS: MEDIA_SHARED_CACHE_READ_ENABLED=false disables all shared source-cache
reads BEFORE Redis-index or filesystem access. Default is true, retaining v1/v2
lookup compatibility. Keep MEDIA_SHARED_PERSISTENT_CACHE_ENABLED=false as well.
Remove NAS's media-shared/canonical-archive bind mounts; absolute configured roots
may remain absent/unused. NAS uses its original own spool download/send/cleanup,
while shared account rate limits and send receipts remain active. No widened NAS
permissions or dependency on NUC's private 0700 directories/0600 files. The tradeoff
is a fresh transient NAS download instead of shared-byte reuse; there is still
one persistent control archive, and no persistent NAS cache duplication.

Deployment preflight: original owner/month/comment directories are UID 0 mode 0755,
so the NFS-mapped writer cannot currently create files there. Live direct-download
acceptance remains blocked until the parent obtains approval and supplies only the
narrow required directory write grant. This release changes no permissions/ACLs;
trusted file ownership is validation policy, not a substitute for write access.

## Verified real-batch wire shape and acquisition cleanup

Read-only private processing of EXACT retained updates 356787102..356787111 found
all ten use major 4 / minor 61, flags 33554434, a 25-byte file reference, media-field
offset 36, body length 64 and a compact 12-byte Thumbnail suffix. No real file IDs,
references, access hashes, media IDs or decoded bytes were logged or saved. The
legacy-only reader incorrectly expected volume/local IDs; it returned fallback for
all ten. The corrected strict compact shape now decodes all ten original photo IDs
in-process, zero fallback, without any media API calls or database mutations.
The existing original DOCUMENT-type stem formatter uses that photo ID; Pyrogram
v2.0.106 types/messages_and_media/photo.py:119-122 confirms the naming identity.
Tests use synthetic IDs only, with the observed structural lengths and a known
nonprivate golden photo ID. Unknown/truncated/wrong-type suffixes remain fallback.

Download tempfile acquisition now records the created FD/path inside its owned
worker before handoff. Cancellation joins acquisition and shield-joins cleanup,
closing the FD and unlinking only the matching owned inode. Tests cancel actual
inbound and MTProto Engine.execute calls after mkstemp creation but before worker
return, including repeated cancellation, and verify absent temp plus EBADF.


### Flat permanent downloads

The current requested layout uses CONTROL_ARCHIVE_FLAT=true: inbound and MTProto
files share one permanent TG_Download media folder with the pinned original logical
filenames. No new owner/month/group subfolders. New manifests stay in private local
control state, not that public root. Existing nested cache files remain compatible;
there is no automatic historical migration/deletion. See original-downloader-storage.md
for deterministic collision-only suffixes and exact-owned-inode cleanup.


## Latest correction: one named folder and one status message per album

CONTROL_ARCHIVE_NAMED=true supersedes root-flat output for the current service:
chat/topic named folder with media directly inside; no month/post/album hierarchy.
Private state/manifests remain outside the public media root. Original naming and
physical alias behavior are documented in original-downloader-storage.md.

Each received item remains its own bounded durable download task, but status is grouped
by exact owner + bot + private chat + Telegram media_group_id. Singles use message ID.
A persisted 2-second quiet window coalesces natural arrivals; late members edit the
SAME stored anchor (maximum Telegram album size 10). Two albums yield two status
messages, never 20 per-item notifications and never a time-only merge. Aggregate text
shows total/done/errors, and owner/chat/reply-bound task action buttons remain available.
The existing preview worker ticks this persisted UI queue; no extra timer service.

The first-send intent is committed before Telegram RPC. Unknown/failed first-send
outcomes or a process death before message-ID persistence are marked ambiguous and
never blindly sent again. A known anchor can be edited after restart without another
send. Cancellation/shutdown leaves durable membership/state for recovery. Existing
DONE tasks/old status messages are not re-enqueued or deleted retroactively. Progress
notification failure cannot cause a media re-download/send.


## Fresh received download versus cached resend

A current authenticated incoming photo is explicit permission to download that current
Bot copy; historical missing files or old-layout records do not require trash restore
or an original-post link. Only typed missing-file/reference or old-layout reuse failures
permit this path. Corrupt/truncated, symlink, foreign/group-writable, wrong namespace or
identity-binding failures still reject BEFORE get_file. Cached-send/forward guards and
zero-source-fetch behavior are unchanged. No MTProto lookup is introduced.

All-terminal album statuses now say download failed / partially complete as appropriate;
queued/delayed retry tasks say processing. The existing single status anchor is reused.

### Retry exactly existing FAILED incoming tasks (operator, quiescent)

The existing recover_inbound CLI now accepts --retry-failed; no new bot/poller/API:

    python -m tg_forwarder.control.recover_inbound --plan <private-plan.json> --retry-failed
    python -m tg_forwarder.control.recover_inbound --plan <private-plan.json> --retry-failed --apply --quiescent

The plan remains version=1 with owner_uid, bot_id, exact configured db and 1..32 updates.
Each retry entry MUST contain exactly:

    {"update_id":123,"payload_sha256":"<SHA256 of retained updates.payload>",
     "task_id":"<existing failed task ID>",
     "task_payload_sha256":"<SHA256 of exact tasks.payload>","expected_attempt":1}

The example placeholders are NOT a deployable plan. Use exact observed private rows.
The CLI requires a process-owned regular plan file with no group/other access; apply
requires the configured bot-token principal, exact state DB, quiescence and the SAME
nonblocking state flock as runtime. It verifies the DONE retained update hash, decoded
authorized inbound payload, owner/chat/bot, exact task key and payload hash, FAILED
state/attempt, plus persisted album membership. ALL rows validate before a single
transaction invokes Store.retry. Existing task IDs, payloads, progress, received updates,
offset, files, receipts and album reply anchor remain unchanged. Only native failed-task
retry state/due/error fields reset; cumulative attempt is preserved, so the worker
claim advances attempt 1 to 2. The identical expected-attempt-1 plan also refuses
a later FAILED attempt 2, not merely queued/running replay. DONE, ambiguous, active, wrong-owner/bot or
changed rows refuse; replaying a plan cannot reset queued/running work. Apply does not
send messages or execute downloads: restart the same service to process the same tasks
and edit their existing album anchor. This is NOT restoration/re-enqueue of old DONE
photos, and no production retry was performed by source tests.


## Album-level actions and mixed received media

The primary album card now has ONE aggregate details control and, only when every
member is a safely cached completed photo, ONE send-whole-album control. Tokens bind
owner, authenticated bot, chat, exact stored anchor, member task IDs and payload revision.
Late media membership invalidates older controls. Details update the SAME album card;
no ten per-item button rows/messages. Cached-upload tasks have a distinct stable album
origin (not the first member's task ID), retaining the existing send claims/receipts,
protection/target guards and zero source/GetFile fallback. 2..10 compatible photos use
one send_media_group. The user must click; nothing uploads automatically. One separate
durable upload-status card stores its reply ID; unknown first-send outcomes never
blindly create another card, and the original download card is not overwritten.

Owned incoming grouped video/document/animation/audio is classified BEFORE original-post
parsing. Unsupported message IDs/type/size/group are stored in private metadata, not
fake download tasks. The same exact album status includes them: e.g. two photos done
plus five videos not downloaded is 2/7, never a completed 2/2 album and never a partial
"send whole album (2)" control. Genuine hidden text/original-link warnings remain; no
text association is inferred from timing. Received typed videos up to the existing Cloud 20 MB limit now use the SAME Bot
get_file transport; no new credentials/endpoints/MTProto lookup or size-limit workaround.

Runtime invokes Controller.refresh_inbound_templates once per album-actions-video-v3 revision:
only the TWO most recent existing known owner/bot anchors (reply>0, not ambiguous/sending/
failed) are marked for edit. At most 32 retained DONE received updates are inspected to
backfill unsupported media ONLY for those exact groups. This changes no update/offset/
task/download state and creates no unknown anchors; it does not replay updates or delete
old warnings. Verify the actual mixed card denominator after deployment; if relevant raw
updates are outside that bounded retained window, stop and request an explicit targeted
metadata plan rather than silently scanning history or inventing membership.


### Received videos and mixed cached albums

Typed SDK message.video is accepted only in the authorized owner private chat with
verified runtime bot ID. A bounded known-version file-ID header checks video type/DC
ONLY; it does NOT decode a bot ID or claim an immutable media ID. The exact received
SDK fields (including file_id, unique ID, size, message/update principal, dimensions,
duration, filename/MIME) are bound by the private payload digest. Current get_file must
match unique ID/size and return a video-type header. Cloud >20,000,000-byte input is
rejected before get_file and shown once in its exact album diagnostic. Photos retain
their existing payload/naming/binding format unchanged. No document/animation support
is added. Video naming uses the existing document/video formatter with SDK filename;
absent filenames use a bounded video_<message-id>.mp4 fallback, labelled truthfully.

Videos use no photo physical alias. Exact-message video caches have an atomic private
per-message fingerprint/binding record; cached reuse/upload verifies it with existing
path/UID/mode/size checks, and successful private manifests include SHA256. New downloads
retain the existing owned temp, bounded response, fsync and no-clobber publication.
Aggregate cached uploads validate ALL completed typed members (photo/video), order by
received message ID, and use the existing mixed InputMediaPhoto/InputMediaVideo group
transport and durable operation receipts. They never call get_file/source RPC.

Recovery for the five already-retained received videos uses the EXISTING version-1
plan/CLI, WITHOUT --retry-failed and without any new config:

    python -m tg_forwarder.control.recover_inbound --plan <private-exact-five-plan.json>
    python -m tg_forwarder.control.recover_inbound --plan <private-exact-five-plan.json> --apply --quiescent

Entries remain exactly {update_id,payload_sha256}; owner_uid/bot_id/db and existing
quiescence/token/flock guards remain required. ALL retained hashes/media payloads
validate before mutation. Controller and CLI share register_payload under their native
transaction: five new queued video tasks attach to the existing photo album notice,
clear ONLY those same video IDs from unsupported metadata, retain its known reply ID,
and bump generation. Repeating apply creates no duplicate tasks/members. Original two
photo jobs, retained raw updates and polling offset remain unchanged. Apply registers
only; restart the SAME service to download the five current Bot copies and edit the
existing seven-member anchor. The startup template refresh must not reclassify supported
videos as unsupported. Apply the exact five plan while quiescent BEFORE service restart;
verify five real DONE/video files and the existing 7/7 card, not health alone. No media
is uploaded unless the user clicks the complete mixed-album cached-send button.


The v3 template refresh also persists exact supported-but-unqueued retained video
membership placeholders (message/update IDs, raw payload SHA256, received binding),
without creating tasks/updates or changing offsets. Thus the historical two-photo +
five-retained-video card is 2/7 awaiting recovery, with NO partial whole-album button.
Native registration verifies and clears those exact pending bindings atomically; already
registered video tasks are never counted twice. Cached access now checks every AVAILABLE
exact photo alias proof (owner/bot/variant/namespace, canonical alias cache reference,
path/size/fingerprint), including legitimate aliases referencing a different message's
cache key on the SAME canonical path. No proof is invented for legacy unindexed rows,
no aliases are rebound during validation, and corruption blocks before any upload claim.
