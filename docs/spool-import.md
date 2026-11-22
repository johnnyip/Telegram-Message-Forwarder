# Download bot: import a saved spool notice

An authorized owner forwards a Saved locally notice containing one path
/app/downloads/_spool/new/<chat>/<token>_<filename> to the control bot.
The existing allowlist and private-chat boundary remain unchanged.
No Telegram re-upload, public URL, original-source lookup or compression occurs.

Control registers durable intake in its local SQLite inbox and submits a bounded
(128 pending), principal-bound request through the existing shared Redis namespace.
A NAS sender-side worker alone opens the private spool ledger. Do not weaken
NFS permissions or open that SQLite ledger from the NUC control container.

## Configuration

Both services require the shared Redis bridge, matching REDIS_PREFIX, and the new
source image. On NAS APP_MODE=send only:

- Bind /volume1/video/TG_Download to /app/import-downloads read/write with
  create_host_path: false; preserve /app/downloads and all other mounts.
- SPOOL_IMPORT_ARCHIVE_ROOT=/app/import-downloads
- SPOOL_IMPORT_ALLOWED_USER_IDS must match the existing verified control owner.
  An empty value fails startup rather than allowing everyone.

The archive root must already exist; never create a fallback. Without the archive
setting, the sender has no new worker. The bot reports an explicit unavailable
error if no worker heartbeat exists. No control allowlist/target changes required.

## Safety and semantics

Only ledger-owned finalized token/paths are accepted. The worker serializes with
spool GC, checks inode/attributes/free space, records a durable checksum intent,
copies to a unique temporary file, fsyncs and SHA-256-verifies, and publishes
without replacement. Collisions, symlinks, hardlinks, changed files, missing roots
and uncertain claims fail closed. Replays verify the durable archive receipt;
they do not overwrite or re-download files.

The bot distinguishes moved (verified archive and native quarantine cleanup),
copied_pending (archive verified but original retained for outstanding delivery
references), and error (no confirmed handoff). Unknown/failed-recoverable/future
references never become false Telegram successes and claims/offsets never reset.
Once references become terminal, normal GC may remove the source; forwarding a
fresh notice also rechecks cleanup eligibility. The supplied >50 MB sample has
a failed-recoverable album receipt, so expect copied_pending, not a false move.

Redis requests/results are durable in the shared namespace. Lifecycle archive
receipts survive Redis eviction. If the archive is subsequently removed, another
forward reports reconciliation rather than silently recreating the file. Native
quarantine recovery remains idempotent after crashes.

## Validation

Run python -m unittest discover -s tests -p test_spool_import.py -v, lifecycle and
control regressions, then the full offline suite. Deployment verification must
read actual image revisions, NAS archive mount, matching Redis namespace, worker
heartbeat and unchanged owner/targets, followed by a real authorized bot update.
Native fixtures are not real owner updates or production delivery receipts.
