# Legacy workbench -> named folders: native design/inventory proposal ONLY

Status: NOT IMPLEMENTED; no migration, downloads, restore, rename, deletion, permission
changes or new authorization is performed. Future fresh URL/received layout parity does
not itself meet a request to reorganize old visible folders. Current missing-only repair
still pins the original cache path. It MUST NOT be described as canonical retargeting.
Existing storage_migration is a historical chat/month + v1-object migration and cannot
be used as-is for named private manifests/aliases or missing files.

## Exact requested cohort and current evidence boundary

Parent-reported historical task 2eef228467a21836 selects post 4739/comment 23508:
53 media (46 photos, 7 videos), main 10 + discussion 43. Last read-only inventory reported
53 cache rows, 0 present / 53 missing after the user removed the entire old folder.
This document is not a fresh scan and does not assume that still holds. Container base:
/media/TG_Download/workbench/-1004433596337/2026-11-10_4739/; recorded NAS base:
/volume1/video/TG_Download/workbench/-1004433596337/2026-11-10_4739/.
Recorded main album 14354633412004628 had 10; discussion albums 14354634889136540,
14354634947877940, 14354635004828164 and 14354635060882676 had 10 each, and
14354635640195604 had 3. These are cohort constraints, NOT permission to scan other
folders or infer source identities from folder names. Never substitute test-fixture
chat IDs, the receiving private chat or the control bot for a discussion source.

## 1. Read-only native inventory and missing inputs

Ops can prepare a bounded exact inventory using the existing sole service's owned
inspection/state path; do not start another user session, poller or unauthenticated
helper. Retain secrets/file IDs/access hashes only in their existing private state;
public output contains counts/relative paths and a plan digest, not raw authorization.

For EVERY selected item record privately:
- Current authenticated control bot principal and requester owner/private chat; original
  task ID, immutable payload/snapshot digest and cumulative attempt/history. Do not infer
  bot ID from an encoded file ID or reuse a stale owner constant.
- Typed original source chat/message/media revision, section, group/album ordering,
  expected size, original Document filename/photo naming metadata, caption/protection
  snapshot; exact cache key/path/size, manifest item and all reverse references.
- Metadata-only present/missing/unsafe classification using root containment, no symlink,
  regular-file, configured UID/mode and size guards. Hash only PRESENT candidates during
  a separately approved proof inventory; missing bytes cannot have a fabricated hash.
- Confirmed original main-channel title and actual discussion-group identity/title.
  Existing snapshot metadata may be absent/numeric; a folder label alone is not proof.
  Obtain missing metadata via the EXISTING authorized read-only --inspect socket/source
  resolver, scoped to the exact original source identities. Compare current source
  chat/message/media IDs and type with each original selection; do not silently replace
  deleted/changed media with a new post, forwarded copy or a larger current thread.
- Forum status, actual topic IDs and verified topic titles through the existing bounded
  GetForumTopicsByID resolver. Do not infer a topic from a comment ID or use channel title
  for its discussion group. If only a CURRENT renamed title can be confirmed, label it as
  current and seek the owner's choice rather than inventing a historical name.

Still-required inputs: actual private snapshot/manifest and payload revision; confirmed
main/discussion titles/topic metadata and original filename metadata for all 53; current
availability/size/proof and destination collision inventory; exact root/mount mapping;
all active references/operations; explicit user approval for the exact mapping AND,
separately, any missing-byte source downloads. Current absence does not authorize them.
Source access failure/protection remains a boundary, never permission to bypass policy.

## 2. Pure mapping dry-run (proposed native extension, not an existing CLI flag)

Project each original item to the current pinned filename() and named chat[ - topic]
label under cfg.root. Main and discussion use their own confirmed identities. Read the
existing archive_folder bindings to resolve sanitized-title collisions deterministically;
read exact cache ownership before selecting collision-only physical-key filename suffixes.
The projection must be PURE: current primary_destination writes folder-binding meta, so
it must not be called as a purported read-only dry-run. A small pure projection helper
would be required; do not invent an executable --canonical-repair flag today.

Emit a complete bounded plan before mutation: owner/bot/task/payload revision/root,
per-item source/key/type/size, old relative path, proposed canonical relative path,
proof if present, folder binding, collision outcome, reference list and requested action.
Show summaries grouped by old/new folder and statuses, retain exact rows privately, and
hash the canonical plan. ALL rows (including the last one) must validate or the whole
plan refuses. Reject unsafe/foreign destinations, conflicting shared namespace, missing
metadata/size, source revision drift, unknown occupied suffix and unclassified references.
No mandatory new upstream hash is invented for legacy rows lacking one; available retained
proof MUST be verified. Never adopt unknown same-size bytes or collapse distinct files by
name/size. Same source/message key is not interchangeable with a Bot-copy physical alias.

Classify separately: already canonical; present valid old file; missing old file; unsafe/
changed item; collision/blocker. With 53 still missing, the result is a 53-item DOWNLOAD
proposal and zero existing-file moves, not a successful organization or reuse claim.

## 3. Separate explicit operations, history and references

A. PRESENT-file organization (separately authorized future implementation): quiesce all
relevant readers/writers, retain the existing native Store/FLOCK ownership, back up private
state, freeze exact plan hash, reverify proof/metadata and destination guards. Stage an
owned same-filesystem no-clobber publication, journal its exact inode/fingerprint, then
CAS-update only inventoried active cache/reference bindings. Keep the old name until all
new bindings and restart verification succeed. Retirement is a separate explicit approved
phase, never implicit cleanup. No cross-filesystem copy fallback or Trash restore.

B. MISSING-only canonical repair (separately authorized future implementation): derive a
NEW download-only task bound to original task/payload + exact plan hash + selected missing
IDs + prior repair head. Its canonical destination is explicitly frozen by that approved
plan, NOT silently substituted into current pinned repair entries. Show old -> new paths
and exact download count before confirmation; preserve original DONE payload/history,
cumulative attempts, old manifests as historical evidence and every delivery receipt.
Recheck whole selection before registration and each exact item under download_lock.
Reject a path that appears while queued/awaiting the lock unless this exact derived task
has independently verified own-publication evidence. Apply current source type/revision,
size/cancellation/temp-inode/fsync/no-clobber guards, receipt-bound restart reuse and
post-await/final-DONE verification. No forward/upload action or receipt replay is implied.

Before either operation can be implemented, enumerate ALL reverse references: current
cache rows, private active manifests vs immutable historical manifests, photo aliases /
video proofs, shared v2 references if configured, any v1 object actually present, and
repair heads/entries/publication receipts. Refuse plans with active/ambiguous sends or
queued/running repairs bound to the old path; do not rewrite an existing repair task to
fit a new layout. Keep task IDs/payloads/forward receipts immutable. Historical manifests
can remain byte-for-byte records of the old location only if ALL live readers use the
explicit verified current binding; otherwise the plan must include their exact active
manifest rewrite and prior/new digest, never leave a live pointer knowingly broken.
New derived task manifests use private local state and record old/new provenance.

SQLite/filesystem/Redis cannot commit atomically together: require a private per-plan
phase journal, CAS old bindings, idempotent resume and revalidation after every phase,
following the established migration primitives, not a new generic migration framework.
Do not retire any old name until every affected live reference verifies. On conflict,
fail closed and report the phase; do not auto-rollback by deleting a sole verified copy.
Latest live Ops verification reports NAS cache reads enabled and persistent payload
publication disabled. Inventory its existing bindings before planning any reference
change; this plan authorizes no mount/config changes. A separately chosen read-disabled
policy would need no cache/archive mount merely to participate in Redis receipts.

## Acceptance before any future apply

Offline tests must cover all 53 mappings; exact main/discussion/topic authority; changed
last row => zero registration; occupied-path/held-lock/publication races; proof corruption;
missing source revision; collision/symlink; whole-plan hash; root/owner/bot mismatch;
cancel/restart at every publication/reference phase; immutable original tasks/receipts;
no API during dry-run; and missing-only transfer count vs fresh/reused counts. Existing
file organization needs zero source downloads; missing-only repair needs exactly the
approved current-source items, never speculative recovery. Finally perform separately
authorized native dry-run, owner confirmation and bounded apply/readback, verifying actual
files and references, NOT just a DONE database count. No such apply is authorized here.
