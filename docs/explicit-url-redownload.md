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
