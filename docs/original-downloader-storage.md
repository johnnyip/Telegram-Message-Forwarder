# Original downloader workflow compatibility

## Provenance and compatibility boundary

Naming helpers are adapted from tangyoha/telegram_media_downloader commit
fe7ccdc2cc5fce5b04a464bc66cfb1266bf7ef45:
- utils/format.py: validate_title and truncate_filename
- media_downloader.py: _get_media_meta, _is_exist, temporary/final download workflow
- module/app.py: default prefixes/separator/date format

MIT copyright (c) 2019 Dineshkarthik R; full notice:
licenses/tangyoha-downloader-MIT.txt. No Pyrogram auth/client is instantiated. The
Telethon adapter extracts DocumentAttributeFilename.file_name, source chat title,
source message datetime, MIME extension and photo ID. Photo unique IDs follow the
stdlib-compatible DOCUMENT-type zero-RLE/unpadded URL-safe-base64 algorithm from
tangyoha/pyrogram 51a100c5e2745471ee89c1dd96dd69962973108b. Golden:
6154650511710734502 -> AgADpsgxGwi2aVU.

This supports original **defaults** for workbench photos/videos, not every upstream
voice/audio/caption/custom-prefix setting or its own bot UI. Filename stem sanitization
and NFC/230-byte truncation preserve upstream behavior; directories retain Unicode.
Missing filenames produce ID.ext. Unsafe paths and no-clobber finalization are deliberate
security hardening over upstream. No arbitrary user writes/symlinks are trusted.
CONTROL_ARCHIVE_TIMEZONE defaults explicitly to UTC provisionally; original runtime
local timezone, historical chat titles and installed configuration still need parent
confirmation. Do not silently switch to HKT or guess missing historical names.

## Current correction: chat/topic folder, media directly inside

Set CONTROL_ARCHIVE_NAMED=true for the current service. It implies flat media
INSIDE one named folder, not flat directly in TG_Download. Existing
CONTROL_ARCHIVE_FLAT=true may remain; NAMED selects the new layout. Library/other
roles retain default false. Folder is sanitized original chat title; when a verified
forum topic exists it is "chat title - topic title" in ONE directory component.
There are no year/month/post/album/owner subdirectories. Original filename() remains
unchanged. The name and 230-byte truncation use the pinned sanitizer/formatter above.
Distinct source/topic identities that sanitize to the same directory label get a
collision-only deterministic suffix, bound in private existing SQLite meta.

MTProto forum names are retrieved by the existing authorized client's bounded
messages.GetForumTopicsByID request (verified against installed Telethon). Missing
verified topic names fail closed rather than merging topics into the chat folder.
Received Bot API photos never trigger MTProto lookup: a genuine Telegram channel
forward_origin.chat provides a folder label ONLY, never actor authority. Hidden/user
origins use the already received, authorized private-chat display name. Missing names
use a neutral private-chat label, not invented original-source identity. Original
origin metadata remains distinct from receiving owner/bot authorization.

All existing root-flat files, nested 87.6 MB original, cache paths, manifests,
per-message task IDs and receipts remain untouched/readable. No relocation, deletion,
renaming, ACL/chmod/chown or mass migration occurs. A preexisting named directory that
is not writable remains an explicit operator access blocker, not permission to bypass
it. New manifests remain in private local CONTROL_DB parent /manifests.

### Received-photo physical aliases (not content/name deduplication)

With flat or named mode, exact owner + authenticated bot + selected file_unique_id +
verified decoded immutable photo ID (when supported) + size + dimensions + intended
chat/topic classification identifies a
physical photo variant independently of message ID and changing file references.
Repeated received messages keep separate durable tasks and cache keys but reuse ONE
known canonical path without another get_file/download. Different bot/owner/variant,
size or revision cannot hit that alias. Authorization and declared/decoded file binding
are checked before lookup. Unknown-size input conservatively does not use aliases.

The private existing SQLite meta/cache transaction commits the new message cache row
and alias binding together. Local size/hash proof detects later file alteration; it
is NOT an upstream Telegram hash assertion or a global content-dedup index. Existing
secure cache path/UID/mode/regular-file checks apply. Corrupt/truncated/changed, symlink, foreign-owner/group-writable or identity-mismatched
indexed data fails closed with NO download fallback. Missing data/reference and old
layout are typed reuse-unavailable outcomes, not corruption. No unknown
file is adopted because its name or size happens to match.

A one-time bounded bootstrap checks only the latest 32 private task rows (at most 10
items each), requiring an authorized DONE inbound payload and exact existing cache
binding. Pre-alias tasks without dimensions additionally need exact selected unique
ID, size and verified immutable photo ID. It never scans the NAS or all task history;
older unindexed sources may download once before gaining an alias. Existing duplicate
files are not moved/deleted. A NEW authenticated received-photo download is independent of restoring historical
files: if a known reuse candidate is missing or uses the old root-flat layout, the
current received Bot file_id may be downloaded through existing principal/unique-ID/
size validation into the intended named folder. It does not restore/copy/move old
data. Its new message cache row and observed-stale alias CAS update are atomic; old
per-message cache rows, receipts and files/trash remain unchanged. Concurrent alias
changes refuse replacement. Another fresh message for the same valid photo then
reuses that new canonical file without another get_file/download. Cached-send/UI
old-cache actions remain strict zero-source-fetch and fail if old data is missing. MTProto cross-message aliasing is deliberately NOT claimed:
its existing authenticated cache/source-revision flow remains unchanged.

## Historical root-flat option (superseded for current service)

Set CONTROL_ARCHIVE_FLAT=true for the current control service. All NEW MTProto
and private inbound photo downloads then publish directly in MEDIA_ARCHIVE_DIR
(the existing TG_Download root): no owner/chat/month/group/discussion directories.
Other roles/library callers retain their existing default false; this is a strict
true/false setting, not a new storage framework. No chmod/chown/ACL change is made.

The SAME pinned filename() codec remains authoritative: message-ID prefix, original
Document filename/extension or decoded photo stable unique ID, original sanitization,
NFC and 230-byte truncation. Modern minor-61 photos use the existing verified codec;
unknown versions retain the explicitly labelled stable fallback. Flattening removes
ONLY directory components, not original filename semantics. On an actual collision
with an unknown/different physical identity, append " - <16 hex physical-key digest>"
before the extension, retaining the byte bound. This necessary collision-only
exception avoids overwriting unrelated same-name documents/photos across chats.
Unknown same-size files are NOT adopted as completed downloads. A colliding suffixed
name fails closed, never grows a numbered series and never overwrites it.

Manifest metadata goes to the EXISTING private local control-state parent of
CONTROL_DB, under manifests/, never the public 0777 media root. The state directories
must be process-owned/private and local; secrets/file IDs remain sanitized. All
partial downloads remain task-owned temporary files on the destination filesystem;
publication is atomic no-clobber and verifies the exact owned inode. Cleanup removes
only that owned inode, never an unrelated replacement. An untrusted writable root
can cause denial/replacement, but cannot authorize an unknown file by its size.

Existing cache paths, nested originals (including the known 87,623,746-byte video),
canonical references, manifests, task IDs and receipts remain readable/unchanged.
An already cached old path is reused, not copied into a second flat payload. No
migration, relocation, deletion or historical directory cleanup is performed by
this setting. The exact migration procedure below remains separately authorized
operator work and MUST NOT be run merely to enable flat new downloads.

## One permanent payload

Control canonical files are permanent original-format downloads. Shared indexes point
to them with {v:2,owner:canonical,root:control,path:<relative>,size,sha256}. The local
MEDIA_SHARED_ARCHIVE_ROOTS_JSON supplies allowed root mappings; control additionally
registers MEDIA_ARCHIVE_DIR and rejects mismatches. Canonical references have no TTL.
File metadata/hash/copy work is dispatched through owned bounded I/O workers; canceled
copy/publication waits for its owned worker before cleaning its own temporary file.
A shared-index failure never deletes a completed canonical download.

Legacy v1 objects remain **read/migrate-only in production**. The configured bridge
now defaults MEDIA_SHARED_PERSISTENT_CACHE_ENABLED=false; parent will set it explicitly
false in both NAS and NUC Git configurations. With this flag, legacy publish returns
its caller-owned spool path without copying or indexing it. The real legacy caller
ignores that return value, uploads the spool normally and uses its existing cleanup.
A transient spool is never registered as a canonical reference.

Control-first -> legacy reuses the v2 canonical reference without redownloading.
Legacy-only -> normal transient download/upload/cleanup, with no additional permanent
payload. A later explicit control download may fetch once if the old spool is gone;
there is no claim of preserving transient bytes forever for future reuse. Old v1 lookups
and explicit hash-guarded migration remain compatible; no new production v1 blobs are
created. Direct constructor callers retain their historical opt-in-compatible default
for tests/library use, and can explicitly pass persistent_cache_enabled=True.

The previously added bounded manual gc_owned is an optional operator tool for marked
legacy objects, **not a lifecycle guarantee**. Nothing depends on someone eventually
running it to clean new production copies, because those copies are disabled. Unknown
old objects still require explicit inventory/migration. There is no GC daemon, lease
framework expansion or new scheduler, and canonical files are never GC targets.

## Exact one-file migration

The source implementation has **not** run against production data. Parent/operator
must inventory exact cache key/source identity, task IDs, old archive/shared object
paths, original metadata, every affected manifest, permitted roots, filesystem UID
permissions and timezone. The existing owned --inspect socket now includes
archive_metadata; no second user-session client is needed.

Use docs/storage-migration-plan.example.json as a schema, replacing all placeholders.
The known two 87,623,746-byte objects have SHA256
7d48e14a28e800a87fa8c8bf8cf136d03751a4fa6c7e015f4b05bd99e699dda9;
that hash does not identify a chat, message, media revision or historical filename.

Dry-run is the default, with no source API calls or shared-index writes:

    python -m tg_forwarder.storage_migration --plan /private/exact-plan.json

Stop **all** control/legacy readers and writers before apply. The existing control
state lock is also taken; a live control owner causes refusal. Supply the deployment's
existing Redis environment and canonical-root mapping, without printing credentials:

    python -m tg_forwarder.storage_migration --plan /private/exact-plan.json --apply --quiescent

Phases: verify hashes/owner/identity; publish canonical destination without clobber;
update matching SQLite cache path; publish rewritten explicit manifests into
canonical .metadata/<same verified task filename> without clobbering;
adopt the source index as v2; verify all references. Private local-state JSON records
each phase, including metadata_relocated, and original/rewritten manifest SHA256 proofs. Tasks, snapshots, operation/send receipts and callback identities remain
unchanged, preserving the already-downloaded photo-send feature. Re-running the same
plan resumes idempotently by revalidating on-disk facts, not trusting a phase blindly.
All SQLite/filesystem work in the async tool is off the event loop.

Publication is same-filesystem atomic link plus eventual old-name retirement. There
is no silent cross-filesystem copy fallback. Before retirement, retained old names or
the already-existing second object are intentional crash safety, not a new permanent
cache. Review verified canonical references, keep all readers stopped, then explicitly:

    python -m tg_forwarder.storage_migration --plan /private/exact-plan.json --apply --quiescent --retire-old

Retirement hashes again and removes **only** the two exact planned old payload paths
and explicitly planned old manifests, after canonical/DB/relocated-manifest/index
verification. Apply without --retire-old retains every old manifest byte-for-byte;
retirement never precedes verified no-clobber metadata publication. Same-plan recovery
accepts verified relocated metadata after old manifests/root are gone. Changed old
metadata, colliding/symlink destinations or missing durable relocation proof fail closed. It never removes a sole verified copy,
unknown media or canonical destination. Hash mismatch, collision, symlink or unexpected
cache/index state aborts. Optional prune_dirs lists at most 32 exact owned old-file ancestor directories
(strictly below old_root, depth at most 16). Only verified-empty listed directories
are removed, deepest first; nonempty/unlisted directories are untouched. There is no
recursive walk/deletion. The tool never removes old_root; the parent may rmdir it only
after it is empty and all old-root binds are gone. Back up local state/metadata first. The plan must list every
affected manifest; the tool does not perform an unbounded NAS scan. Reader quiescence,
complete inventory and root/mount mapping remain external operator responsibilities.

## Tests

Offline golden names include observed photo and ZIP patterns, missing names, Unicode,
byte truncation and month boundaries. Tests cover one permanent payload, legacy spool
reuse, missing/hash-tampered references, root traversal, explicit GC age/index/lease,
dry-run, cache/manifest preservation, collision/symlink/live lock and restart recovery
after every migration phase. No test needs production credentials or Telegram calls.

## Actual service namespace / protected environment integration

The migration CLI loads CONTROL_ENV_FILE on --apply with the existing runtime
load_dotenv(override=False) policy before constructing the shared bridge. It does not
instantiate a Telegram client or another Store/state owner. Default dry-run does not
read that protected environment file or contact Redis. Credentials are not included
in the normal result JSON.

Run migration only as a temporary entrypoint of the **same existing Git-owned NUC
control service**, with its existing container paths/state/secret mounts, after all
NAS and NUC readers/writers are stopped. Restore the normal Git-controlled service
entrypoint afterward. Do not run host-native CLI against container DB paths, create
a parallel helper owner, clone authentication or invent path mapping.

Parent's intended steady-state bindings/settings (not applied by this documentation):
- Both services: MEDIA_SHARED_PERSISTENT_CACHE_ENABLED=false.
- NUC: MEDIA_ARCHIVE_DIR=/media/TG_Download;
  MEDIA_SHARED_ROOT=/media/TG_Download/.tg-media-cache;
  MEDIA_SHARED_ARCHIVE_ROOTS_JSON={"control":"/media/TG_Download"}.
- NAS legacy service: bind that hidden .tg-media-cache to /media-shared, bind the
  canonical TG_Download root read-only to /download-archive;
  MEDIA_SHARED_ROOT=/media-shared;
  MEDIA_SHARED_ARCHIVE_ROOTS_JSON={"control":"/download-archive"}.
- Retain access to exact old cache/archive paths only through the quiescent explicit
  migration. No resumed post-retirement process should still bind old_root.

Parent verified the existing NAS cache mount/namespace, not a current canonical-root
mount. Canonical bindings, complete metadata inventory and live service configuration
still require normal Git/CI deployment. Timezone remains unknown; the current Nov 11
file stays in 2026_11 across the relevant zones, so explicit provisional UTC need not
block this one file. This is not confirmation of original runtime timezone.

## Explicit NFS file-owner mapping

MEDIA_ARCHIVE_FILE_OWNER_UIDS=0,1024 explicitly admits known original root-owned
files and server-mapped NFS UID 1024 in addition to the existing process/root-owner
policy. UID 0 is not globally trusted by default. Canonical validators, bridge
references and migration use the same bounded numeric policy; symlinks, empty or
nonregular files and group/other writable files remain rejected. No chmod/chown/ACL
changes or parent-owner inference. Existing original directories may still require
an independently approved narrow write grant; the whitelist does not grant access.
See [inbound-photo-downloads.md](inbound-photo-downloads.md).


### Current 20-file organization: existing migration CLI gap

No organization was executed by this change. The existing storage_migration.Migration
constructor currently hardcodes canonical_media.relative() (chat/month), canonical
.metadata manifest relocation, and an exact legacy v1 shared-object path. Therefore
it does NOT directly support the current 20 root-flat inbound files -> new named
folder + existing private-state manifests/physical aliases as-is. Do not invent a
plan that pretends those v1 objects exist. Its established hash/no-clobber/quiescent/
SQLite/reference-verification primitives can be reused by a separately authorized
narrow explicit-plan extension/operator procedure. That procedure must inventory all
20 distinct files/keys/manifests, preserve every duplicate (no dedup deletion), backup
private state, verify reverse mapping and update known references atomically. The
old 87.6 MB file is outside that 20-file operation. This source change adds no new
migration framework and performs no NAS moves, copies, deletes or permission changes.
