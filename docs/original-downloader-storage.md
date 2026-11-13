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
