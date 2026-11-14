# Explicit URL download resubmission

Submitting/querying a URL and explicitly selecting Download (or Download +
Forward) now inventories actual cache files when native dedup finds an existing
DONE original source task. Safely missing files create one derived download-only
round automatically: no extra location/repair button is needed. Merely posting
a URL for preview, rendering a card or restarting a worker does not create work.
All-present submissions return the existing historical task and current cache
summary, never a fresh-download claim.

The original ID, payload, progress, attempts and forwarding receipts remain
unchanged. Active, failed, cancelled and ambiguous repair heads are reused with
native controls; no blind reset/retry occurs. Once a round finishes, another
explicit submission after another deletion creates a new round. Concurrent
submissions cannot create duplicate rounds.

Fresh query items must match original media identities, kind, album, section and
size. Confirmed metadata determines native NAMED chat/topic folders and original
filenames. Missing legacy workbench paths are not recreated. Unsafe cache paths,
missing metadata, conflicting folder identities, occupied/symlink destinations,
changed source identity or denied source access fail closed. A queued round
rechecks pinned old/cache/destination state after acquiring the download lock;
only its exact inode/hash-bound publication receipt permits replay reuse. Native
no-clobber finalization and final receipt checks remain in force.

Even resubmission of Download + Forward produces a DOWNLOAD-ONLY recovery round;
it never replays historical forwards or uploads. Existing cached-send behavior
and compact progress cards are unchanged. No production media was fetched by the
offline regression tests.

This feature does not migrate existing files. Partial alignment.py,
test_alignment.py and the unstaged alignment-plan checkpoint are separate,
unreleased work, not part of this release or its completion evidence.

## Main-source folder grouping (new downloads only)

For a source URL, the resolver's authoritative main entity owns the archive
folder: main-post media and its linked discussion/comment attachments use the
same confirmed main-channel title (and main-source topic when applicable).
A comment URL still resolves its main post first; the discussion group's title
is not a fallback archive root. A standalone group URL keeps that group's own
confirmed title/topic. Missing main title/topic authority fails closed rather
than guessing a folder. Incremental comment snapshots revalidate main authority.

Grouping changes naming metadata only. Each attachment retains its actual source
chat/message/media identity, access hash, protection flag, source topic and
original filename metadata. A separate folder-topic binding prevents a discussion
forum topic from splitting the main-source archive. Identical projected original
filenames from distinct source identities receive deterministic identity suffixes
in the same folder. Existing native occupied-name disambiguation and no-clobber
publication remain in force; an occupied second candidate fails closed.

Existing completed files/cache rows are neither moved nor re-downloaded. An
explicit resubmission with all files present keeps those historical locations.
If files are safely missing, the previously documented download-only round uses
the newly resolved common main-source root. No migration or operator repair is
triggered automatically. Paths shown by location controls remain actual verified
cache paths, not an invented new location for historical files.

## Whole requested scope, not a planning-time missing subset

A fresh explicit ALL submission that finds missing files now creates one derived
DOWNLOAD-ONLY task whose selection is the complete original authoritative scope.
For example, 31 missing photos plus 22 apparently present items remain a 53-item
request, not a misleading 31/31 completion. Both fresh-query and original media
identities/kind/album/section/size must agree. Existing active/failed/ambiguous
heads still use native dedup/controls; no history or previous payload is rewritten.

Existing files are retained at their actual cache locations, pinned by exact
inode/device, UID, mode, mtime and SHA256/size evidence. They are not downloaded
again merely to produce a total. If they safely disappear during planning or
transfer, that same task can download the missing identities into the current
canonical main-source folder. Foreign replacements, attribute/hash drift, unsafe
paths, permission/IO errors and occupied new destinations fail closed. Only an
observed disappearance is retryable; ACL/identity failures are not converted into
missing-file authority. Publication still needs native owned-file receipts.

Before DONE, the worker checks the full original selection. Files that disappeared
after earlier reuse receive at most two additional recovery sweeps in the same
head/task, each remaining download-only. Continuous deletion exhausts the bound
and fails, rather than claiming full completion. A disappearance during an
individual verification gets at most three same-task attempts; unrelated failures
are not retried through this path. Known missing coverage lowers progress and is
retained in the private manifest's coverage/unfinished fields. Successful manifest
coverage must match the complete requested count. Fresh/reused statistics count
unique selected identities, not repeated recovery checks.

The original bug was scope truncation: a one-time missing inventory became the
entire derived task and final validation only checked that subset. Historical
31-item and22-item DONE rounds remain historical receipts, not proof that a
53-item target had been validated in either round. The code cannot reconstruct
unlogged external-deletion/NFS timing and does not attribute such timing to a
particular cause. No production repair, move or source probe is part of this fix.


## New source-download folder policy (2026-11-15)

Only new source task allocations use this policy. Existing present cache paths,
completed task payloads, files and receipts are retained in place: no migration,
copy, rename or unsolicited download. A deleted-file resubmission allocates a new
repair head but keeps all original logical identities and full selected scope.

- More than one distinct logical media identity in the FULL selected scope:
  MAIN_TITLE/YYYYMMDD_HHMMSS-task8/original_filename. The timestamp is task
  creation in Asia/Hong_Kong; the short stable task suffix and payload persist
  atomically with the new task. One missing member out of 53 remains a 53-member
  batch. All newly downloaded members share that directory even across original
  months; there are no month subdirectories inside it.
- Exactly one distinct source media identity: MAIN_TITLE/yyyy_MM/original_filename,
  using that media's original aware Telegram message.date, including discussion
  messages. Neither resubmission/download time nor a forward timestamp substitutes
  for the original instant. Missing, invalid or naive timestamps fail closed.
- Linked discussion items retain their own source identities, access/protection
  checks and filenames; only the authoritative main-source naming root is shared.
  Standalone group URLs retain their own authoritative root. Batch duplicate names
  across chats/months get deterministic source-identity suffixes.
- Batch directory creation is exclusive, fsynced and bound to the native task,
  scope and directory inode/attributes. Retries/restarts keep the allocation and
  verify its stored claim. Occupied foreign directories and changed claims fail
  closed, never get adopted or overwritten. A crash between mkdir and durable
  claim fails closed rather than treating an unproved directory as owned.
- Existing present members of a repair are verified and reused in place, not copied
  into the new batch merely for visual uniformity. All newly acquired members use
  the new batch folder. Path cards read actual cache paths, not a guessed layout.

### Upstream formatter and timezone evidence

Pinned tangyoha/telegram_media_downloader commit
fe7ccdc2cc5fce5b04a464bc66cfb1266bf7ef45:
module/app.py:394,416,717-747 defaults to the prefix hierarchy
chat_title/media_datetime and date format %Y_%m;
media_downloader.py:203-211 derives media_datetime from
message.date.strftime(app.date_format) rather than the current clock.

Pinned Pyrogram adapter evidence at
51a100c5e2745471ee89c1dd96dd69962973108b:
pyrogram/types/messages_and_media/message.py:1049 derives date from the actual
message timestamp, separately from forward_date at line1081;
pyrogram/utils.py:502-503 uses datetime.fromtimestamp(ts), hence the original
formatter's timezone is the PROCESS-LOCAL timezone, not intrinsically UTC.
The downloader requirement points to a moving Pyrogram patch branch, so this is
adapter provenance, not a claim of an immutable upstream dependency lock.

This Telethon adapter deliberately uses the existing explicit
CONTROL_ARCHIVE_TIMEZONE for single-file month boundaries. Observed configured
value was UTC; the legacy container's separate TZ=Asia/Hong_Kong does not
authorize silently rewriting that setting. No timezone/config changes are made.
Batch timestamps use the separately specified Hong Kong task-creation clock.

### Completion barrier

Full-scope final verification hashes every retained or freshly published member
and captures its device, inode, size, mtime, ctime, uid and mode. A single bounded
attribute batch is checked after the last asynchronous hash verification and
before the synchronous DONE transition, with no intervening await. Same-inode,
same-size corruption, including restored mtime, is rejected by the ctime/attribute
barrier rather than misclassified as missing and redownloaded. Only proven
missing members enter bounded recovery under the same native head.

Native future-new-write spool lifecycle is described in spool-lifecycle.md.
Neither implementation nor the separate historical discard authorizes age-only
unknown deletion. This document makes no deployment claim. The archive layout
policy above applies to source-link downloads; bot-inbound feature flags and
layout are unchanged.
