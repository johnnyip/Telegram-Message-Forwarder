# Opt-in shared media bridge

Implementation: tg_forwarder/shared_media.py. Native tests do not imply live acceptance.
Defaults: MEDIA_SHARED_ENABLED=false; include_discussion_media=false on every route.

## Current deployment boundary

Latest live Ops verification reports NAS shared_cache_reads=true and persistent payload
publication=false (MEDIA_SHARED_CACHE_READ_ENABLED=true and
MEDIA_SHARED_PERSISTENT_CACHE_ENABLED=false). Verified existing cache/canonical references
may therefore be reused; fresh legacy downloads remain transient download/send/cleanup
without publishing another persistent payload. Shared Redis rate/send receipts are
independent. Exact mounts, root labels and permissions require their own live inventory;
these flags alone do not prove a mapping. Examples below do not authorize configuration
or mount changes. Disabling reads is a separate optional hardening policy, NOT the
current deployment. NUC control keeps its canonical archive and private local state.

## Lazy configuration

Factory reads environment at call time AFTER environment()/CONTROL_ENV_FILE, including Redis credentials; no import-time credential snapshot.

- MEDIA_SHARED_ENABLED: true/false only, default false.
- MEDIA_SHARED_ROOT: absolute operator-owned root for OPTIONAL byte-sharing consumers. When reads are enabled, matching physical storage may use different container paths. Old workbench/shared-cache and /media-shared mappings are historical examples, not current NAS mount requirements. Read-disabled NAS needs no such bind. No symlink components or untrusted writers.
- MEDIA_ACCOUNT_RATE_KEY: required nonsecret account label shared across processes using the same user account. Conservatively sharing different accounts is allowed. Never use a session string or token.
- MEDIA_ACCOUNT_RATE_INTERVAL=.1: shared admission slot spacing seconds, not byte-bandwidth control.
- MEDIA_DELIVERY_IDENTITY: legacy shared upload identity. APP_MODE=send derives bot:{authenticated_bot.id}:legacy-mirror-v1 from its existing bot.get_me() startup probe when unset. Explicit matching-principal semantic suffixes are preserved; mismatched kind/ID or empty suffix fails startup. Never infer from destination owner or use a token.
- MEDIA_NATIVE_DELIVERY_IDENTITY: native MTProto identity. APP_MODE=listen derives user:{authenticated_user.id}:native-v1 from its existing client.get_me() startup probe when unset. Explicit matching-principal semantic suffixes are preserved; mismatched kind/ID or empty suffix fails startup. It is NOT the target owner or send bot identity. Native header and forward have separate operation receipts.
- Existing REDIS_HOSTNAME, REDIS_PORT, REDIS_DB, REDIS_USERNAME, REDIS_PASSWORD, REDIS_PREFIX (default tgfwd). No credential values in source/logs.
- MEDIA_SHARED_MAX_BYTES=2147483648; MEDIA_SHARED_MIN_FREE_BYTES=1073741824.
- MEDIA_SHARED_INDEX_TTL=2592000 (30 days): only Redis source indices expire. Archive files NEVER auto-expire/delete. Re-publication safely reindexes immutable existing objects.

Shared mode PRESERVES the configured native large-media direct-forward path (including media larger than Bot API upload limits), using immutable document/photo revisions from fetched source messages. Native header and native-forward actions have separate receipts: a definite forward rejection can retry without duplicating an already sent header. No shared cache download/re-upload is involved in native forwarding. Protected-content rejection still reaches the original legacy fallback policy. Old upload snapshots without shared_identity still need deliberate re-snapshot; old native jobs fetch current source messages and can use revision-aware native claims. OFF keeps old behavior.

## Historical v1 adapter example (not current control integration)

Current control publishes its finalized permanent file with publish_reference(identity,
path), a v2 reference rather than another persistent payload. Current NAS reads are
enabled; valid configured references may supply source bytes. The following v1 example
applies only to compatibility callers, after protected environment loading:

    from tg_forwarder.shared_media import configured_bridge, media_identity, message_identity
    bridge = await configured_bridge()  # None disabled; exceptions are NOT bypass
    identity = message_identity(telethon_message)
    # Alternatively immutable Resolver document/photo media_id, not caption timestamp:
    identity = media_identity(item['chat'], item['id'], item['media_id'])
    path = await bridge.materialize(identity, owned_exclusive_staging_path)
    if path is None:
        path = await existing_download_with_policy_checks(...)
        await bridge.publish(identity, path)
    # Feed owned staging path to existing controller archive/copy; not shared path.

Other signatures:

- await cached(identity) -> Path | None
- await publish(identity, source_path) -> Path
- await materialize(identity, owned_spool_path) -> Path | None
- operation(identities, destination, topic, delivery_identity, *, action='upload', caption='') -> str
- await deliver(operation, async_zero_arg_send_callable) -> original sender result OR restored result
- await receipt(operation) -> dict | None (empty dict also means success)
- await wait(); await record_delay(seconds); sync delay(seconds) compatible with Resolver gate.

Optional v1 materialization COPYs cache bytes into an owned spool. Current control
v2 publication keeps its one permanent file. Read-enabled NAS may copy verified shared
bytes to its transient spool; persistent publication remains disabled. Cleanup/unlink/retention never deletes shared archive or mutates its bytes. Publication copies, fsyncs, atomic no-clobber link and directory fsync; task-owned exclusive incoming file alone is cleaned up. Filesystem validation/copy/fsync execute through asyncio.to_thread with a shared two-worker admission semaphore per event loop, across bridge instances. Cancellation joins shielded owned work before returning: publish completes durable file plus Redis index (or propagates its failure), materialize removes only its own exclusive partial/completed spool copy after the thread finishes. No detached filesystem worker races caller cleanup. NAS outage can delay cancellation until its kernel filesystem call returns; do not promise instant cancellation of a blocked mount. Redis stores relative flat hash filename and size, never cross-host absolute paths. Size/symlink/traversal guard failures are fatal. Existing objects never overwritten. Operator-trusted immutable cache, not adversarial-storage checksum verification; malicious filesystem writers are outside trust boundary.

## Receipts, identity and safe failure

    operation = bridge.operation([identity], destination_chat, destination_topic_or_none,
        'bot:6317800517:control-upload-v1', action='upload', caption=exact_final_caption)
    result = await bridge.deliver(operation, async_send_callable)

Downloading source bytes is NOT proof of sending. Keys include complete source identity set (chat/message/media revision), destination, topic semantics, delivering identity, action, and final caption. Changed captions create distinct actions; cache reuse never suppresses required edits. Legacy completed replay restores sender message IDs and still writes existing edit mappings. Original journal behavior remains.

Different bots intentionally differ: editing authority/visibility differ. Legacy uses topic object {legacy_sender: sender_id, policy: FORUM_TOPIC_DELIVERY_MODE default mirror, edit_revision: edit_date for edited jobs, otherwise null}. Control usually uses numeric topic or None. Thus NO unsafe cross-bot, mirror-versus-single or album-versus-single suppression. Equivalence exists only when all fields intentionally match. Albums cover the entire source identity set; NOT item-level reconciliation across differently grouped uploads. Current control target192114587 versus legacy groups naturally differ. This is a common conservative ledger, NOT full exactly-once across differing actions.

Claims use SET NX without TTL. Only successful delivery writes completed receipt. Crashed, concurrent, cancelled, transport-failed, false or uncertain sends stay blocked indefinitely as ambiguous: never retry merely because time/process elapsed. Definite Telegram RetryAfter/FloodWait/BadRequest/Forbidden rejections release only the current owner token using compare-and-delete Lua, permitting a safe retry. The send callback must represent one atomic external effect: if it has already sent any earlier effect before a later rejection, it MUST wrap that later error as an uncertain RuntimeError instead of leaking a definite-rejection exception. Native header/forward are split specifically to honor this rule. Legacy upload fanout wrappers conservatively return false on uncertain failures, retaining claims. A successful size-limit notice or best-effort album fallback is not proof all media arrived: preserve legacy result for retention/mapping but keep claim without media receipt. Degraded general-only delivery when an intended topic exists likewise does not certify topic delivery. Optional sender result attributes sent_message_id(s), general/topic IDs, delivery_kind, message_thread_id and preserve_local_copy serialize for replay.

Operator must inspect actual destination and explicitly reconcile exact claim/done keys. No automatic admin deletion/retry API. Claims/receipts have no TTL: deliberately audit/retain, monitor Redis memory, enable persistence. Redis loss/reset/eviction destroys dedup evidence. GET/SET NX EX/PX/EVAL only under existing tgfwd namespace; EVAL already used by legacy mapping. Redis missing/unavailable/ACL failure propagates; no fail-open send. After claim before receipt any failure is ambiguous.

## Cross-process rate coordination

Pass gate=bridge to Resolver(client, ...). Before each user-account RPC/download, await bridge.wait(); after FloodWait await bridge.record_delay(seconds). Legacy main.run_api performs both, including retry gate. Sync delay schedules an async write, joined and errors propagated by next wait: use a long-lived bridge. Controller delivery callbacks with internal retries must gate EACH retry and record matching-account FloodWait/RetryAfter. Gating an album callback once is not per-RPC throttling. A delay cannot revoke already in-flight calls. No errors suppressed. Manual/controller jobs remain separate from Kafka.

## Optional route discussion snapshot

Add include_discussion_media:true as a strict JSON boolean on intentionally selected routes; all existing routes remain false. Schedule after qualified main publication, once per source post/album (not each album event), opt-in destinations only. Text posts qualify. No second main dispatch, group history monitor or extra session.

Lazy existing Resolver uses exact linked post/root plus bounded GetReplies, excludes source/root album/auto-forwarded copies. Ordered photos/videos grouped into existing publish_media_job/publish_album_job, source_kind='discussion', discussion_source={chat_id,post_id,thread}. Existing ignore, skip and download/forward/protected-policy remain in force; no bypass.

One active scan, eight queued/in-flight snapshots, 10000-message scan limit, 300-second scan+publish deadline. Ordinary receive handler does not await scan. Capacity, partial, timeout and route failure log discussion_snapshot_* errors; main post is not rolled back. Timeout after some jobs may leave partial published set: no automatic retry; inspect exact jobs/destination before deliberate replay. Per-process 10000-post completed LRU plus existing legacy event/album dedup; not a durable watch cursor or exactly-once multi-listener coordinator. Use the existing single authorized listener. Snapshot at event time only; does not monitor future comments.

## Verification

Existing native venv: python -m unittest discover -s tests -v. No containers or real Redis/Telegram connections. tests/test_shared_media.py invokes actual legacy snapshot/media-file pipeline and exact AST-extracted main.run_api body, plus actual Resolver/discussion scheduler with 60-media thread fixture. Covers default OFF, cache reuse and cleanup isolation, target/topic/bot/caption/revision separation, ambiguous failure/cancellation, Redis-down failure, notice!=media, inherited rate delay and strict route flag. Rerun full suite after concurrent controller edits before publication. No live acceptance claimed.

Review regressions: targeted tests additionally cover event-loop responsiveness during blocked copies, cancellation-safe index publication, owned materialization cleanup, two-worker IO cap, definite rejection release versus timeout uncertainty, and native >BotAPI media forwarding with no download plus header receipt preservation. Public bridge APIs remain compatible.

Authenticated startup binding: under shared mode the existing get_me probes must succeed, return a valid positive principal ID, and agree with any explicit identity. Binding happens before producer/consumer startup; listen event handlers are registered only after binding and producer readiness so no early bridge call can use an unverified principal. Probe failures are fatal under shared mode, not warning-continue. Default OFF retains previous warning/continuation behavior and does not set identity variables. No cloned authorization or fresh session is needed. Tests/test_shared_startup_identity.py executes pure binding helpers and actual startup branch bodies (unrelated shutdown flushing isolated) to verify ordering and failure boundaries.

## Original-format canonical references (v2)

Control no longer copies completed archives into permanent hashed objects. It publishes
root-labelled relative-path/size/SHA256 references. MEDIA_SHARED_ARCHIVE_ROOTS_JSON
maps permitted names to trusted local mount paths in each process; configure control
to the original-format TG_Download root. V1 hashed source lookup remains compatible.
Rate/send-receipt behavior is unchanged. See original-downloader-storage.md for explicit
legacy retirement/GC and dry-run-first one-file migration. Never delete unknown hashed
objects ad hoc: they may be the sole surviving copy.

Production transient policy: MEDIA_SHARED_PERSISTENT_CACHE_ENABLED defaults false for
configured_bridge and should be explicitly false in both services. Legacy fresh spool
publication becomes a no-copy/no-index operation; existing upload/spool cleanup remains
unchanged. V2 canonical publication/reuse and old v1 reads/migration still work. Manual
GC does not substitute for this policy or guarantee legacy object cleanup.

## Independent legacy cache reads

`MEDIA_SHARED_CACHE_READ_ENABLED` defaults to `true`. Explicit `false` returns before any filesystem or Redis source-index access; shared rate and send-receipt coordination remains enabled. Use together with `MEDIA_SHARED_PERSISTENT_CACHE_ENABLED=false` on NAS and remove cache/archive binds rather than widening private NUC permissions. NAS uses its original owned transient download/send/cleanup path. See [inbound-photo-downloads.md](inbound-photo-downloads.md).
