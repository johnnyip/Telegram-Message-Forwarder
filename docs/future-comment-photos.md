# Future comment-photo forwarding (legacy routes, opt-in)

This is a separate source change from received-photo/control/storage alignment.
It reuses the existing listener USER client, Kafka broker/producer/media sender implementation, spool,
shared account gate and upload receipts. It creates no Telegram session, media
archive, persistent blob cache, bot command, join/read acknowledgement, or new UI.

## Enable only an existing confirmed broadcast-channel route

Add the strict JSON boolean to the existing route; preserve its sources/targets
and all other routes/settings:

    {"name":"existing-route","sources":[-1004433596337],
     "targets":[-1003836445993],"forward_future_comment_photos":true}

The numbers are an existing candidate configuration, NOT proof that discussion
access is available. Parent/operator must confirm the existing route and linked
discussion before authorizing deployment. No source/destination is accepted from
chat text. Disabled/absent defaults to false. Explicit numeric channel sources
are required; usernames, private chats and arbitrary groups are not enrolled.

The existing Redis provider/ACL and MEDIA_SHARED_ENABLED=true are REQUIRED for
this lane. Shared per-target upload receipts are necessary to handle Kafka ack
uncertainty and duplicate entrypoints safely. Missing Redis/receipts or forbidden
source access fails only the new background lane; ordinary forwarding continues.
MEDIA_SHARED_CACHE_READ_ENABLED=false / persistent cache disabled in the newer
core release remain compatible: rate/receipts, not shared media bytes, are used.
No change to shared_media.py, the control engine or canonical archive is needed.

Do not enable include_discussion_media for this feature: it remains the separate
old one-time snapshot option and can include historical comments. This new flag
never asks for a historical whole-thread snapshot.

## Effective activation and scope

After producer startup, a bounded background task verifies the channel's linked
discussion and the group's reciprocal channel link, then saves the group's current
highest message ID as its fresh baseline. Activation is effective only when this
baseline is durable (active=true, enabled_at recorded), not merely when an env
variable was edited. No message at/below that baseline is published. If access is
missing, state records an error and activation retries without backfilling data
that existed before its eventual successful baseline.

Later photos in comments on OLD channel posts as well as NEW posts are eligible.
The collector reads only newer group IDs and lazily resolves the comment's root;
it never enumerates old posts or scans old threads. Existing per-post workbench
watches still work separately and are not a prerequisite.

Only photos are projected; video/audio/document/text-only messages do not create
new forwarding jobs. A mixed album emits its photo members in source order with
those photos' existing captions. Automatic forwarded outer/root album members,
unrelated group posts, another channel's roots, unverified forwarded-author
metadata, forbidden/unknown roots and cycles are not blindly treated as comments.
Nested replies resolve a verified top root or a bounded ancestor chain (16 hops).
GetDiscussionMessage and the existing bounded album resolver verify actual root
membership, including every member of an automatic outer album.

Parent channel, original post, group and current photo protection checks fail
closed. Existing sender ignore/skip policy and destination/topic semantics remain
in force. Current channel/group protection or failed lookups preserve unresolved
cursor progress rather than silently skipping that interval. A proven protected
individual photo is not enqueued. No native-forward fallback is introduced.

## Polling, albums, restart and backpressure

Live NewMessage updates only wake the background collector. No source RPC,
download or mapping is awaited in the receive handler, including when ordinary
route matching would return because the incoming chat is the linked group.
Correctness does not depend on receiving live group updates: passive GetHistory
reads newer than the persisted group cursor recover new-only gaps if allowed.
This does not join a group or mark history read.

Defaults: 15-second background interval, 30-second album stability, 50 input
messages plus one boundary probe per channel/tick, at most 32 enrolled/retained
channels. Source RPC calls have a 20-second bound and each collector tick a
110-second bound. A 120-second named Redis lease fences state writers. No Redis
SCAN/KEYS is used; only the bounded named channel registry and explicit keys.

The extra history item detects a page-spanning album. Its truncated tail is NOT
published. The oldest unresolved album must have the same membership/revision
signature for the settling interval; stability survives restart. Input IDs are
sorted, so out-of-order update arrival does not affect source ordering. Oversized
albums, inaccessible ancestors, changed pending revisions and source failures
surface bounded state errors without advancing past unresolved input. Backlog
continues in bounded oldest-first pages, not a mass historical backfill.

### Isolated future Kafka lane (operator-provisioned topic)

Future jobs use exactly <KAFKA_MEDIA_TOPIC>-future-comments and consumer group
<KAFKA_CONSUMER_GROUP>-future-comments. The ordinary text/media topics, groups,
commit batching and legacy failure policy are unchanged. The future supervisor
contains every failure (including startup, malformed JSON and missing files);
it cannot escape into the ordinary consumer gather or stop normal forwarding.
Each lane owns its own consumer and offset batcher, using the same implementation.

The topic MUST already exist and be accessible. The code does not provision it.
Before publication/subscription it requests ALL-topic metadata (never named unknown
topic metadata or partitions_for). An absent/unauthorized topic pauses only the
future lane. Topic creation/permissions and any live proof are operator work.
With all flags off and no prior registry there is no new consumer, metadata client,
future task or topic request. A bounded named registry check is used only when the
existing Redis provider is configured. Known disabled generations still reconcile.

Before publication, the exact photo IDs/media revisions and the intended cursor
advance are durable. The existing Kafka adapter stages its exact prepared job
bytes (maximum 1 MiB) and actual FUTURE topic in that pending record before
send_and_wait. Every intended photo must have a matching revision-bearing
snapshot; missing single snapshots or partial albums cannot enqueue a subset.
Each file is sealed with actual size and SHA-256 off the event loop. An uncertain
ack replays those same bytes after restart, without another snapshot/download.
The cursor advances only after acknowledgement or proven irrelevant input.
An acknowledged selection remains pending until every intended target has a
concrete complete shared delivery receipt. Only one selection per source may be
outstanding; no further collection can outrun unresolved delivery/spool lifetime.
Existing consumer journal identity includes future policy generation and physical
revisions; existing shared upload receipts independently arbitrate actual sends.
Unknown/partial delivery outcomes remain reconciliation-required, never a blind
resend. A failed future-comment consumer job stops ONLY the separate future consumer,
with its offset unacknowledged and a persistent named blocked marker. Normal
forwarding remains healthy. The marker pauses future collection and prevents an
automatic restart/resend. Resolve/reconcile uncertain sends before explicitly
clearing that marker. A processing/done journal entry is NEVER sufficient evidence
to skip/ack future work: all expected physical-selection/target receipts must be
confirmed, or that offset remains held without transport. Empty dispatch returns,
partial album IDs, fallback notices and missing topic copies are not completion.

Before any future transport, EVERY prepared file must still exist as a regular
non-symlink file with its sealed size/hash. All/partial missing album spools and
same-size replacements fail without transport, done marking or offset advance.
This is not permission to redownload replacement media or shrink the selection.

Prepared records survive broker acknowledgement, restart, disable and target
generation changes. Canceled records retain their exact payload/prior status;
cancellation does not assert an ambiguous earlier send never happened. Evidence
transitions use bounded raw-value Lua CAS against the latest status with immutable
prepared bytes. Only an accepted prepared-to-in-flight handoff may dispatch;
already-canceled selections cannot be resurrected. A cancellation after handoff
records cancel_requested while preserving in-flight uncertainty. Confirmed complete
status dominates stale cancellation, and repeated terminal operations are idempotent. Each source
retains at most 32 terminal evidence references; only explicitly complete records
may be retired using a crash-resumable exact-key marker. Permanent shared receipts
remain. Uncertain/canceled evidence is never swept: reaching its bound pauses that
source for explicit review. There is no unbounded payload queue or key scan.
Source/index/state records have no automatic destructive cleanup or media sweep.

## Disable / targets / equivalent sends

Set the route flag false (or remove it) and deploy the same configuration to the
listener and sender. Startup reconciles known disabled states once using the
existing Redis provider, even when all flags are false; it performs no source
reads for disabled sources. A disabled generation fences pending collection while retaining canceled/uncertain evidence.
Re-enabling or changing selected sources/targets establishes a NEW baseline;
disabled-period backlog is not sent. Already-acknowledged/started Telegram sends
cannot be undone.

Checks occur after awaited mapping, while staging, before consumer claims and
immediately before transport. Current route allowlists and persisted generation
must agree; old queued work cannot inherit new targets. A state read outage before
transport is a typed definite rejection so it cannot poison an uncertain-send
claim. Ordinary jobs do not enter these extra sender checks.

Equivalent photo selections delivered by the same authenticated bot, to the same
target, with identical caption and sender-topic/mirror semantics use the existing
physical-revision shared receipt, regardless of normal-group/new vs discussion
entrypoint or route name. Different destinations are separate operations. A
workbench's plain-caption/private send, another bot, a native forward, or a mixed
photo+video album vs a photo-only subset are different delivery semantics; those
are NOT claimed to be globally identical or retroactively deduplicated. Keeping
one equivalent route/snapshot policy avoids intentional non-equivalent copies.

## Inspect / acceptance

Named state keys use <REDIS_PREFIX>:future_comments:channels and
<REDIS_PREFIX>:future_comments:<signed channel ID>; error values are exception
class names, not credentials or private RPC URLs. Inspect only the specific known
keys through the existing authorized operations path. Additional exact named keys
are <REDIS_PREFIX>:future_comments:blocked and delivery:<context digest> under
that prefix; source state records the bounded evidence references/retiring key. No new admin HTTP endpoint,
credential or production mutation tool is provided.

Offline tests exercise actual main receive/no-route handling, ordinary album flush,
parent-route Kafka album adapter, strict/default-off config, old/new posts, nested
and spoofed roots, root album exclusion, mixed/late albums, page boundaries,
restart, exact outbox replay, changed revisions, target/disable generations,
protection/access failure, lease loss, and shared target-scoped receipt reuse.

Before opt-in: run the merged core+feature suite/CI, confirm the existing bot
identity/receipt settings, verify passive linked-group read access using the
already-authorized client, explicitly provision/authorize the future Kafka topic, and confirm an effective fresh baseline. Only after
explicit deployment/test authorization observe a fresh real comment photo/album
and correlate source IDs -> Kafka job -> intended destination. No live forwarding,
source reads, joins, new messages or permission changes were performed by this
implementation task. Storage/core rollout and the owner's pending archive write
approval are separate and unchanged.
