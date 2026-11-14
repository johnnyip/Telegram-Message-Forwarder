# Native future-new-write spool lifecycle

The listener records new snapshot ownership in a private synchronous-FULL SQLite
ledger under _spool/.lifecycle before writing download bytes. Each acquisition
has an exclusive inode and held lease. Telethon writes into the owned open file,
including shared-cache materialization; cancellation joins writers before closing
and cleaning its exact unpublished partial. No-clobber publication records the
final inode/size/mtime/ctime/uid/mode. Partial albums are retained, not sent as
successful complete albums. Shared archive copies remain independent and untouched.

Before Kafka publication, the same durable transaction binds the complete selected
route/target set and immutable snapshot tokens. Broker uncertainty retains intent;
acknowledgment-bookkeeping failure cannot cause a producer resend. A target records
inflight before transport and only a full native file/album receipt marks complete.
Partial/fallback/failed/uncertain deliveries retain recoverable bytes. Replays skip
already-proven targets; an inflight/failed target requires explicit reconciliation,
not an automatic resend. Route changes fail closed. Send-journal expiry, poison
skips, cancellation, or Kafka offsets alone never authorize deletion. Legacy jobs
without ownership records are retained rather than blindly unlinked.

All-target durable proof releases references after normal sender terminal success.
Cleanup is independently durable cleanup_pending, with a 24-hour grace. Startup
and hourly native sender GC also reconcile crash-after-delivery terminal proof
(non-future jobs only). Future-outbox jobs require explicit terminal release.
Unlink failure is retried independently and cannot turn a proven delivery into a
resend. Durable target receipts survive file deletion and send-journal TTL.

GC is a single flock owner, invoked through cancellation-joined worker I/O, not a
cron or second daemon. Per pass: at most 80 file candidates examined, 64 deletions, 256 MiB and a two-second
cooperative file-work budget (SQLite lock waits have their own five-second bound).
It uses only registered rows, never walks/adopts the spool. Directory-fd nofollow
traversal, private metadata, exact stat identity, single-link checks and fsynced
unlink reject changed files, symlinks, extra hardlinks and unavailable roots.
Known unpublished acquisitions older than 72 hours may be reclaimed only with no
publication reference and a successfully acquired dead lease plus inode proof.
Live writers, failed partial albums and uncertain/queued references are retained.
SQLite metadata aggregation may inspect known receipt rows; the file-work budget
is not a hard deadline for storage latency. No receipt/lease metadata TTL is added.

UNKNOWN historical files are retained indefinitely. The separately authorized
one-shot historical discard is not a recurring policy. No seven-day failed purge,
source API probe, redownload, canonical migration or runtime service change is
part of this source patch. This protects new native snapshot writes; it does not
retrofit trustworthy receipts onto legacy files or change bot-inbound flags.

## Atomic cleanup claims and recovery

An externally mutable source pathname is never unlinked. Linux renameat2 with
RENAME_NOREPLACE atomically claims it into a fresh private0700 quarantine
subdirectory, using held directory descriptors. Unsupported native primitives
fail closed. The claimed inode/attributes and single-link ownership must match
before any payload unlink. Destination collisions never overwrite. Publication
also uses native no-clobber rename, eliminating its link-then-path-unlink window.

A separately fsynced claim intent records token, original relative path,
quarantine location, expected identity and private directory identity before
moving. Verified proof is persisted before private unlink; removed proof before
SQLite terminal commit. Restart retries the same claim, never a replacement at
the source path. Only verified/removed claims can reconcile an absent payload as
deleted. A source missing before any claim is cleanup_uncertain, not proof of
unlink. A raced foreign inode, symlink, hardlink or changed verified payload stays
in its recorded quarantine with uncertain status and no automatic rollback that
could overwrite a new source file. Operator reconciliation can locate both paths
from the receipt; future GC never silently discards these mismatches. Ancestor
changes cannot redirect held-directory operations into replacement directories.
Private metadata assumes normal same-uid process trust, not protection against a
malicious same-uid actor forging the ledger or directly rewriting private files.
