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
