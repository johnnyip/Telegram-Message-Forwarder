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
