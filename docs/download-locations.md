# Native download-location view

Download/both task cards with completed or partial progress offer 📂 下載位置 for
owner 5268216341 in the same private chat. Existing opaque persisted capabilities bind
owner/chat/message; their private payload also binds task ID, exact task payload revision
and verified runtime bot principal. No callback accepts a filesystem path. Stale
revisions, wrong principals and invalid pages refuse.

The view edits the SAME card, with ten filenames per page, bounded parent-folder counts
and total available/missing/unconfirmed counts. It reads only this task's exact cache
keys, then performs root containment, symlink/ownership/mode/regular-file/size metadata
checks in the existing async IO worker. It does not hash/read media, call get_file or
source RPC, resolve original posts, redownload, move or upload files. Missing cache
records differ from missing files; unsafe/outside paths are not disclosed.
Paths are root-relative, including legacy workbench/<peer>/<date_post>/main|discussion
layouts; no named-flat-folder prerequisite or layout migration is introduced. The actual
configured root is labelled program/Docker-local, NOT guessed as a NAS/host mount.
Unicode/angle brackets are plain text (parse_mode=None); callback tokens remain below
Telegram's length limit. Existing details/my-tasks controls remain.

Runtime invokes native refresh for at most 32 known stable task cards, including
owner/chat-bound persisted capability anchors for deduplicated submission cards (a
bounded 256-capability scan). Explicit bot/revision bindings must match; legacy native
navigation requires the retained verified bot principal. Active/ambiguous anchors are
protected. Each successful card edit has its own v2 revision receipt; failed edits remain
retryable on the next refresh, never create replacement messages, and 429 stops the
batch through the existing gate. No hardcoded task/message IDs or synthetic updates.
Existing inbound album/cached-upload cards keep their own notice pipeline. No new
configuration, authentication or mount settings are needed.


## Deleted files and historical deduplication

Submitting the same URL/selection/action retains the existing task identity; it is NOT
proof of a new download. DONE cards now explicitly label historical completion and show
current available/missing metadata counts. Recent execution telemetry separately counts
successful fresh downloads versus reused/skipped cache items; legacy DONE jobs with no
telemetry are labelled as having no new-download record, never invented as this run.

The location view offers an EXPLICIT repair-missing button only for indexed, safely
root-contained MISSING files of a historical original-source DONE download/both task.
Its capability binds the current inventory and repair head; changed inventories/stale
clicks refuse. A native derived download-only task selects ONLY those missing items,
keeps the original DONE payload/progress/attempt/history and forwarding receipts intact,
and pins destinations to the ORIGINAL cached root-relative paths (including legacy
workbench layout). No upload/forward replay occurs. The existing original-source
identity/permission checks and owned-temp/no-clobber pipeline remain mandatory. Unsafe,
wrong-size or unknown-existing files are never selected for overwrite. An originally missing path that becomes occupied fails closed before source access.
Only a fingerprint-bound publication receipt from this exact repair task can authorize
restart reuse; arbitrary same-size owned files cannot be adopted. A crash before its
publication receipt is recorded fails closed for operator review. Repair attempts
use existing cumulative retry counters; active/failed/cancelled/ambiguous repair heads
are shown with native controls, never blindly reset. Repeated deletion after a completed
repair requires a NEW explicit round, not dedup to the previous DONE repair. No repair
is executed by rendering cards, startup refresh or repeating a URL alone.

Repair destination checks run under the download lock. Repair reuse bypasses ordinary
size-only cache adoption and verifies its receipt before/after publication. Fresh repair
publication is reverified after publication and temporary-file cleanup awaits.
Repair completion also revalidates every selected publication after progress/manifest
awaits, immediately before recording DONE under the download lock.
