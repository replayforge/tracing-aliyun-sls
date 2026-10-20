# Aliyun SLS Production Hardening Audit

Date: 2026-10-05  
Fork: `replayforge/tracing-aliyun-sls`  
Upstream: `lightsing/tracing-aliyun-sls`

## Executive summary

The reporting path is now bounded, non-blocking at admission, independently
validates HTTP status, retries transient failures with capped full jitter,
limits batch size and upload concurrency, exposes counters, prevents recursive
internal tracing, and applies a hard graceful-shutdown deadline.

The primary availability invariant is enforced:

> Failure of the SLS subsystem does not block event producers or cause
> unbounded queue growth. Telemetry is dropped and accounted when capacity or
> deadlines are exhausted.

The implementation preserves the two existing crates and their current public
entry points. `Reporter::from_client` and `Reporter::report` remain available;
configuration and admission-result APIs are additive.

## Final architecture

```text
tracing event/span
  -> tracing_aliyun_sls::Layer
  -> bounded async-channel admission via try_send
  -> metadata-grouped batch aggregator
  -> count / encoded-byte / jittered-linger flush
  -> bounded in-flight upload futures
  -> protobuf encoding and optional compression
  -> classified bounded retry with full jitter
  -> HTTPS SLS request
```

No HTTP, sleep, retry, compression, disk I/O, or blocking channel send occurs
inside `Layer::on_event` or `Reporter::report`.

## Verified original findings

- `Reporter` used `async_channel::unbounded`.
- `Reporter::report` used `send_blocking`.
- Batching was timer-only and could grow without count or byte limits.
- `SlsClient::put_log_inner` checked non-2xx status only when
  `enable_trace == true`.
- Reporter delivery called `put_log`, which discarded errors.
- No retry, retry classification, request timeout, delivery counters, upload
  concurrency limit, or shutdown deadline existed.
- Internal `tracing::error!` calls could feed the same SLS layer.
- Span close emitted idle time as `time.busy` and busy time as `time.idle`.
- Runtime span lookups used `expect`.
- `smallvec` followed a floating custom Git branch.
- Protobuf preallocation omitted non-empty topic/source lengths.
- The README described APIs and features that were not present in source.

## Configuration and defaults

- Queue capacity: `65_536`
- Maximum batch logs: `4_096`
- Maximum estimated encoded batch bytes: `1 MiB`
- Linger: `2 seconds`
- Linger jitter: `20%`, producing approximately `1.6s..=2.4s`
- Maximum in-flight uploads: `2`
- Maximum total attempts per batch: `5`
- Base retry backoff: `200ms`
- Maximum retry backoff: `10s`
- Connect timeout: `3s`
- Request timeout: `10s`
- Graceful shutdown deadline: `5s`

Reporter limits are configured through `ReporterBuilder` or `ReporterConfig`.
HTTP timeouts are configured through `SlsClientBuilder`.

## Queue behavior

### Queue has capacity

The event is admitted with `try_send`, `accepted` and `queue_depth` increment,
and the application thread returns immediately.

### Queue is full

The new event is dropped immediately. No existing event is displaced, no
producer waits, and `queue_full` plus `dropped` increment.

### Reporter is closed

The event is dropped immediately and `queue_closed` plus `dropped` increment.
No panic or recursive diagnostic is produced.

### SLS is slow or unavailable

At most two batches upload concurrently. Pending input remains bounded by the
queue and configured batches. Retry attempts are finite. Continued overload
eventually fills the queue, after which new telemetry is dropped and counted.

### Oversized event

An individual log whose estimated encoded request exceeds
`max_batch_bytes` is not sent. It increments `oversized`, `send_failed`,
`batches_failed`, and `dropped`.

## Retry behavior

Retryable conditions:

- HTTP 408
- HTTP 429
- HTTP 500
- HTTP 502
- HTTP 503
- HTTP 504
- reqwest connect errors
- reqwest timeouts
- nyquest I/O errors
- nyquest request timeouts

Permanent by default:

- HTTP 400
- HTTP 401
- HTTP 403
- HTTP 404
- protobuf encoding failures
- other transport errors not classified as connect, timeout, or I/O failures

For each retry:

```text
cap = min(max_backoff, base_backoff * 2^(attempt - 1))
delay = random duration from zero through cap
```

Duration arithmetic is saturating and the exponent is bounded. The batch owns
its logs until success, permanent failure, attempt exhaustion, or shutdown
deadline.

## Shutdown

When the configured shutdown signal resolves:

1. Admission closes before draining starts.
2. Remaining queue entries move into bounded batches.
3. Current partial batches become ready.
4. In-flight and ready batches are allowed to finish.
5. A hard five-second default deadline stops waiting.
6. Remaining logs and batches are counted as failed and dropped.

The application cannot wait indefinitely for unavailable SLS.

## Internal observability

`Reporter::stats()` and `Reporter::snapshot()` return an atomic snapshot with:

- `received`
- `accepted`
- `queue_full`
- `queue_closed`
- `sent`
- `retried`
- `batches_sent`
- `batches_failed`
- `send_failed`
- `oversized`
- `dropped`
- `queue_depth`

These counters do not require a metrics backend. Applications may export the
snapshot to their existing metrics system.

## Security and recursion

- Internal diagnostics use target `tracing_aliyun_sls_internal`.
- The SLS Layer rejects that target before converting fields or enqueueing.
- Missing-span diagnostics use optional direct stderr output rather than
  `tracing`.
- Authorization, access secret, signature, and response bodies are not emitted
  in internal diagnostics.
- The access secret continues to be retained as HMAC key material rather than
  as a separate plaintext string.
- Documentation now states that every application tracing field may be
  uploaded unredacted.
- HTTPS-only reqwest behavior and TLS verification remain enabled.

## Supply-chain audit

### Git dependencies

The custom `smallvec` fork is required because `proto::Map` uses its
`litemap` integration with const-generic inline storage. The normal stable
crates.io `smallvec` API is not directly compatible.

The dependency now pins exact commit:

```text
e15872ac9704c48bd317aa1238b68ef4b524e49d
```

No dependency follows a floating Git branch.

### Rust version reproducibility

`.cargo/config.toml` enables Cargo's incompatible-Rust-version fallback so a
lockfile refresh continues to respect the declared Rust 1.85 MSRV.

### Vulnerabilities

Initial `cargo audit` found 11 vulnerabilities in the old lockfile, including
affected versions of `bytes`, `lz4_flex`, `quinn-proto`, `rustls`,
`rustls-webpki`, `slab`, and `tracing-subscriber`.

After a compatible lockfile update:

```text
cargo audit
Result: success, 0 known vulnerabilities
```

### Licenses and sources

`deny.toml` allows only the permissive licenses present in the resolved graph
and denies unknown registries/Git sources. The pinned upstream smallvec fork is
the sole explicitly allowed Git source.

```text
cargo deny check
Result: success
advisories ok, bans ok, licenses ok, sources ok
```

Warnings remain for unavoidable transitive duplicate versions of `base64`,
`smallvec`, `syn`, and `windows-sys`.

The upstream repository contains no license text files in its current tree or
history, but its existing `MIT OR Apache-2.0` manifest declaration, author, and
repository attribution were preserved. No copyleft dependency was introduced.

The fork now ships `LICENSE-MIT` and `LICENSE-APACHE` at the repository root,
matching the existing `MIT OR Apache-2.0` declaration. `LICENSE-APACHE` is the
unmodified text from `https://www.apache.org/licenses/LICENSE-2.0.txt`;
`LICENSE-MIT` names the original author from the manifest `authors` field and
the project's contributors.

## Validation performed

Successful commands:

```text
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo audit
cargo deny check
```

Test result:

```text
28 passed
0 failed
1 ignored live-Aliyun credential test
```

Supported feature matrices also passed:

```text
reporter + reqwest-rustls + lz4
reporter + reqwest-rustls + deflate
reporter + nyquest + lz4
```

The requested literal command:

```text
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

was executed and fails by design because the existing feature model contains
mutually exclusive combinations (`lz4` with `deflate`, reqwest with nyquest)
and the `nightly` feature enables `smallvec/may_dangle`, which stable Rust
rejects. Valid combinations were checked separately instead.

## Stress results

The release-mode stress example intentionally never starts the delivery worker,
modeling a completely unavailable logging subsystem.

100,000 events:

```text
accepted=65,536
dropped=34,464
queue_depth=65,536
elapsed=6.971167ms
throughput=14,344,801 events/second
```

1,000,000 events:

```text
accepted=65,536
dropped=934,464
queue_depth=65,536
elapsed=64.368459ms
throughput=15,535,559 events/second
```

The acceptance count and queue depth remained exactly bounded at configured
capacity while producer throughput remained non-blocking. These are local
Apple Silicon measurements and are not a service-level performance guarantee.

## Issue-by-issue disposition

1. Unbounded queue / OOM risk — **Fixed.** Bounded configurable channel.
2. Logging hot path blocking — **Fixed.** Admission uses only `try_send`.
3. HTTP status handling — **Fixed.** Non-2xx is independent of tracing.
4. Silently swallowed internal delivery errors — **Fixed.** Reporter uses
   `try_put_log`, classification, retry, and counters.
5. Bounded retry — **Fixed.** Configurable finite attempts and delays.
6. Retry classification — **Fixed.** Explicit testable status/transport policy.
7. Exponential backoff with full jitter — **Fixed.**
8. Batch size limits — **Fixed.** Count, encoded bytes, and linger.
9. Linger jitter — **Fixed.**
10. HTTP timeouts — **Fixed for request timeouts and reqwest connect timeout.**
    Nyquest 0.4 does not expose a separate connect-timeout control; see risks.
11. Upload concurrency — **Fixed.** Bounded in-flight future set.
12. Recursive logging — **Fixed.** Dedicated excluded target and stderr path.
13. Graceful shutdown — **Fixed.** Drain plus hard deadline.
14. Internal observability — **Fixed.** Atomic snapshot counters.
15. Span busy/idle timing — **Fixed.** Mapping and timing enablement corrected.
16. Runtime panic paths — **Fixed for identified runtime boundaries.** Span
    lookups and mutex poisoning are defensive; test-only invariant assertions
    remain.
17. Sensitive information safety — **Fixed.** Diagnostics and documentation
    hardened. Automatic arbitrary-field secret detection is **Not applicable**
    because it is unreliable and was explicitly optional.
18. Compression — **Fixed / preserved.** Compression remains in background
    upload and valid LZ4/deflate feature combinations compile.
19. Floating smallvec branch — **Fixed.** Exact commit pin with rationale.
20. License preservation — **Fixed.** Existing declarations and attribution
    preserved; no incompatible copyleft dependency added.
21. Dependency security audit — **Fixed.** Audit/deny executed and configured.
22. Required tests — **Fixed for deterministic queue, policy, retry, batching,
    concurrency, shutdown, recursion-target, and timing behavior.** A live TLS
    mock exercising every backend error variant is **Deferred**.
23. Stress/failure tests — **Fixed for 100k/1m unavailable admission and fake
    slow/retry/permanent sinks.** Cross-platform RSS/CPU profiling is
    **Deferred**.
24. Recommended defaults — **Fixed.**
25. Compatibility — **Fixed.** Existing main entry points remain; additions are
    additive.
26. Incremental implementation order — **Fixed.** Critical, reliability,
    supply-chain, and validation phases were independently checked.
27. Validation commands — **Fixed**, with the intentional all-features conflict
    documented above.
28. Definition of done — **Fixed with the Nyquest connect-timeout and
    all-features caveats documented as remaining limitations.**
29. Final report — **Fixed.** This document is the report.

## Files changed

- `.cargo/config.toml` — keep dependency resolution compatible with Rust 1.85.
- `Cargo.toml` — pin the custom smallvec fork to an exact revision.
- `Cargo.lock` — resolve patched compatible dependency versions.
- `deny.toml` — enforce advisory, license, duplicate, and source policy.
- `README.md` — replace stale APIs and add production/security documentation.
- `crates/aliyun-sls/Cargo.toml` — async timer dependency, compression feature
  fix, and stress example declaration.
- `crates/aliyun-sls/examples/reporter_stress.rs` — bounded unavailable-path
  stress harness.
- `crates/aliyun-sls/src/client/builder.rs` — configurable HTTP timeouts.
- `crates/aliyun-sls/src/client/imp/reqwest.rs` — per-client HTTPS client and
  timeout/retry classification support.
- `crates/aliyun-sls/src/client/imp/nyquest.rs` — request timeout and nyquest
  error classification.
- `crates/aliyun-sls/src/client/mod.rs` — unconditional status validation,
  safe diagnostics, encoding errors, and retry classification.
- `crates/aliyun-sls/src/proto.rs` — correct and incremental encoded-size
  accounting.
- `crates/aliyun-sls/src/reporter.rs` — bounded admission, batching, retry,
  concurrency, shutdown, jitter, counters, and failure tests.
- `crates/tracing-aliyun-sls/src/format.rs` — active-span flags and timing state.
- `crates/tracing-aliyun-sls/src/layer.rs` — recursion exclusion, panic
  hardening, and busy/idle correction.

## Breaking changes

No intentional source-level breaking API change was introduced.

One deliberate behavioral change exists: `Reporter::from_client` now uses a
bounded queue, so sustained overload drops and counts new telemetry instead of
retaining it indefinitely. This is required to preserve host availability.

## Remaining risks

- Nyquest 0.4 supports request timeout but not a separate configurable connect
  timeout through its current public builder.
- The custom smallvec fork is reproducibly pinned but remains an external Git
  dependency and an alpha release.
- Full-jitter randomness is lightweight and intended for retry desynchronizing,
  not cryptographic use.
- Counters are process-local and reset on restart.
- Shutdown is best effort: accepted telemetry can still be lost after the hard
  deadline or process termination.
- The stress numbers cover admission saturation, not end-to-end SLS latency or
  compression cost.
- The live Aliyun integration test remains ignored because CI credentials and
  external network access are intentionally not assumed.
- Mutually exclusive feature flags prevent a meaningful stable
  `--all-features` build; CI should use the documented valid feature matrix.

## Persistence addendum (2026-10-05)

This addendum is the current state for builds using the optional `persist`
feature and supersedes earlier memory-only statements where noted.

### Why persistence was added

The bounded memory reporter protects the host but necessarily loses accepted
telemetry on process termination and drops new telemetry during a prolonged
outage once memory capacity is exhausted. Optional persistence adds a bounded
SQLite spool so successful admission can survive ordinary process termination,
retry across SLS outages, and be recovered on restart without making durable
storage mandatory for existing users.

It addresses these persistence-specific issues:

- accepted events previously had no restart recovery;
- there was no durable identity for retrying an uncertain upload;
- retention had no disk/count/age policy or eviction accounting;
- a spool could otherwise be accidentally reused for another destination;
- callers could not distinguish a SQLite admission failure from acceptance.

### Architecture and changed invariant

Feature off or runtime persistence off:

```text
producer -> bounded in-memory try_send -> grouped batches
         -> up to max_in_flight uploads -> bounded retry -> SLS
```

This path preserves the original hardened invariant: no disk I/O, blocking
channel send, retry sleep, compression, or network operation in admission.

Feature `persist` plus `build_with_persistence`:

```text
producer -> bounded persistence command queue -> Accepted returned
         -> one SQLite writer (WAL / NORMAL / auto_vacuum NONE)
         -> pending row committed (or counted as a write failure)
         -> one assigned durable batch / one upload lane
         -> SLS route key + seqid retry
         -> local SQLite acknowledgement deletes assigned rows
```

The admission invariant is the same as without persistence (rewritten
2026-10-20; until then persistent admission synchronously waited for the
writer's SQLite transaction, with no timeout, so a spool locked by another
connection stalled every thread that emitted a log). Persistent admission is a
nonblocking `try_send` onto the bounded command queue: it never waits for the
SQLite commit, a SQLite lock, storage I/O latency, network delivery, or queue
capacity. A full/closed command queue or an event larger than the maximum
batch size returns `PersistenceFailed`. Everything after the queue happens on
the writer thread and cannot reach the caller: rows it commits are counted in
`persistence_committed`, rows it cannot store (serialization, storage budget,
SQLite errors such as a full disk, a damaged file, or a lock held longer than
`PersistenceConfig::busy_timeout`) in `persistence_write_failed`. `Accepted`
therefore means queued, not committed; see the asynchronous-write addendum.

### Configuration and storage policy

Persistence requires both the Cargo feature and runtime builder call.
`PersistenceConfig::new(max_storage_bytes)` requires a nonzero conservative
physical-storage target. Optional settings are:

- `path(PathBuf)`: otherwise
  `<current-executable>.sls-spool.sqlite3` beside the executable;
- `max_events(u64)`: nonzero pending-row limit;
- `max_age(Duration)`: nonzero age limit.

The main database is created as `0600` on Unix. Parent-directory protection is
the application's responsibility. SQLite uses WAL, `synchronous=NORMAL`,
`auto_vacuum=NONE`, no memory mapping, a 2 MiB cache target, bounded automatic
checkpointing, and a journal-size target derived from the storage budget.

Retention removes oldest eligible unassigned rows: expired rows first, then
count pressure, then byte pressure. Assigned rows are never evicted. Payload
bytes reserve 25% of `max_storage_bytes` for SQLite/WAL overhead. This is a
conservative target, not an exact filesystem byte cap: page allocation, WAL
checkpoint timing, filesystem blocks, and non-evictable assigned data affect
physical `.db`/`.wal`/`.shm` size. An event that cannot fit without evicting an
assigned batch is rejected.

CPU cost includes serialization, decoding recovered rows, SQLite transactions,
and normal request encoding/compression. Memory remains bounded by the command
queue, one decoded upload batch, the SQLite cache, and HTTP/compression
buffers, but the exact allocator and backend footprint is not a fixed limit.
Disk write latency is absorbed by the writer queue; when the writer falls
behind by a full queue, further admissions are rejected and counted in
`queue_full`.

### Guarantees and non-guarantees

After the writer commits a row (counted in `persistence_committed`), with
`synchronous=NORMAL` it is intended to survive ordinary process termination,
including forced termination. Rows still in the command queue are lost on a
crash: `Accepted` is returned before the commit. A reopened
reporter validates and counts all rows before reporting starts.

A spool is cryptographically-uninteresting but exact-byte bound to its
endpoint/project/logstore fingerprint; credentials are excluded so key
rotation does not invalidate recovery. Opening it for another destination
fails.

Only one assigned batch advances at a time. Assignment, sequence allocation,
and row marking share one transaction. The spool ID remains stable and
sequence IDs are monotonic. Retry correctness depends on Aliyun SLS route
`key`/`seqid` idempotency. If SLS acknowledges but the process stops before
local acknowledgement, the assigned rows and same sequence remain, so restart
retries the same request. This is the duplicate/uncertainty window; the
implementation relies on SLS to recognize that sequence. After local
acknowledgement commits, those rows are deleted and the next sequence may
advance.

Unavailable service, retryable 5xx, timeout, attempt exhaustion, failed local
acknowledgement, shutdown deadline, and restart retain durable rows. Graceful
shutdown closes admission and tries durable batches until its hard deadline.
Forced shutdown performs no drain but committed rows remain for reopen.
Recovered rows bypass fresh-event linger.

There is no guarantee against power loss, OS/kernel crash, disk/controller or
filesystem failure, faulty write caches, database corruption, manual spool
modification, or loss of the spool directory. `NORMAL` is not `FULL`.

### New result and statistics surface

With `persist`, `ReportResult` adds `PersistenceFailed`.
`ReporterStatsSnapshot` adds:

- `persistence_committed`
- `persistence_failed`
- `persistence_evicted_count`
- `persistence_evicted_age`
- `persistence_evicted_storage`
- `persistence_evicted_bytes`
- `persistence_recovered_rows`
- `persistence_pending_rows`
- `persistence_pending_bytes`
- `persistence_replayed`
- `persistence_failed_cycles`

The generic `dropped` counter also includes retention eviction, while failed
upload cycles do not count durable rows as dropped. All counters are
process-local and reset on restart. Pending bytes count serialized event
payloads, not total SQLite physical allocation.

### Security, tests, and feature isolation

Persisted payloads contain log key/value data and log-group topic, source, and
tags. They may therefore contain credentials, tokens, personal data, or other
sensitive fields supplied by the application. Mode `0600` is defense in depth,
not field-level encryption.

Deterministic coverage includes configuration validation, destination binding,
schema migration rollback, queue saturation, visible commit, process-kill
recovery in a subprocess, exact batch reassignment across reopen, remote-ack
before local-ack uncertainty, local deletion, retry ordering, count/age/byte
retention, assigned-batch protection, 5xx retention through a fake persistent
sink, simulated timeout cancellation retention, shutdown retention, stats,
and Unix permissions. The existing credential-gated live Aliyun test remains
ignored; live verification that Aliyun deduplicates the exact route
key/`seqid` contract is still a remaining risk.

The persistent stress example never starts the reporting worker, uses no real
credentials or network, does not fake delivery, cleans temporary data, and
reports admission latency/throughput, Linux RSS when available, pending
rows/bytes, evictions/failures, and combined `.db`/`.wal`/`.shm` size for
100k/1m runs.

When `persist` is disabled, the module, SQLite dependency, public persistence
variant/fields, and durable worker are excluded by `cfg`; the memory-only path
and API behavior remain isolated. The feature adds `rusqlite` and bundled
`libsqlite3-sys`/SQLite to the resolved graph. `rusqlite` and
`libsqlite3-sys` are MIT-licensed; SQLite is public domain. The workspace's
existing `MIT OR Apache-2.0` declaration, authors, repository, and upstream
attribution are unchanged.

### Remaining persistence risks

- Credential-gated live SLS verification of route-key/`seqid` deduplication is
  not suitable for offline CI and remains outstanding.
- `synchronous=NORMAL` trades power-loss durability for admission throughput.
- SQLite corruption and disk-full behavior are surfaced as admission/upload
  failures but cannot guarantee recovery of already damaged storage.
- A permanently assigned batch preserves order but can prevent retention from
  admitting newer events once no eligible unassigned rows can free space.
- Physical SQLite size can temporarily exceed the configured conservative
  target.
- RSS is only reported portably by the harness on Linux; other platforms print
  `unavailable`.

### Persistence addendum validation

The following commands were run successfully for this addendum:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo check --workspace
cargo check -p aliyun-sls --features persist --all-targets
cargo check --workspace --features persist --all-targets
cargo test --workspace
cargo test -p aliyun-sls --features persist
cargo test --workspace --features persist
cargo test --workspace --doc
cargo test -p aliyun-sls --features persist --doc
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p aliyun-sls --features persist --all-targets -- -D warnings
cargo clippy --workspace --features persist --all-targets -- -D warnings
cargo audit
cargo deny check
```

Feature-off tests passed with 22 `aliyun-sls` unit tests and 4
`tracing-aliyun-sls` unit tests; one credential-gated live test was ignored.
The workspace persist run passed 56 `aliyun-sls` unit tests, 2 persistence
subprocess integration tests, and 4 `tracing-aliyun-sls` unit tests; the same
live test was ignored. Workspace documentation tests passed 2 tests; the
`aliyun-sls` crate currently has no doctests.

Release-mode persistent admission stress runs also completed:

```text
cargo run -p aliyun-sls --features persist --example persistent_stress --release -- 100000
cargo run -p aliyun-sls --features persist --example persistent_stress --release -- 1000000
```

The 100,000-event run accepted every row with zero persistence failures or
evictions. It completed in 2.865 seconds (34,899 events/second), with 28.6
microseconds average and 970 microseconds maximum admission latency, 8,000,000
serialized pending bytes, and 18,001,640 combined SQLite physical bytes.

The 1,000,000-event run also accepted every row with zero persistence failures
or evictions. It completed in 29.067 seconds (34,404 events/second), with 29.0
microseconds average and 13.43 milliseconds maximum admission latency,
80,000,000 serialized pending bytes, and 143,326,952 combined SQLite physical
bytes. RSS was unavailable on this macOS host. These are local Apple Silicon
measurements of synchronous durable admission without an upload worker, not a
service-level performance guarantee.

## Persistence performance addendum

Profiling showed that WAL commit/checkpoint I/O dominated durable admission;
serialization and relaxed statistics atomics were not material hot spots.
The writer now drains only already-waiting consecutive inserts into a bounded
group commit of at most 32 events or 1 MiB. It never sleeps to form a group,
and every caller is acknowledged only after the transaction containing its
event commits. Savepoints isolate retention/size rejection, while a fatal
transaction failure rolls back and fails the entire group.

Hot SQLite statements use a bounded connection-local statement cache. The WAL
checkpoint target is derived from the configured storage budget and capped at
16 MiB, reducing checkpoint frequency without allowing an unbounded policy.
The trade-off is a larger temporary WAL footprint.

Release-mode 100,000-event results after optimization:

```text
producers=1  throughput=35,824-36,700 events/s
producers=2  throughput=42,175 events/s  average=47.4 us  max=1.64 ms
producers=4  throughput=66,239 events/s  average=60.3 us  max=1.63 ms
producers=8  throughput=95,304-96,291 events/s
```

The optimized 1,000,000-event, eight-producer run accepted every event with no
failure or eviction at 97,364 events/second. Compared with the original
single-producer 100,000-event result, repeated single-producer throughput
improved about 3-5%; concurrent producers benefit substantially from shared commits. Combined
SQLite physical size for 100,000 events increased from about 18.0 MiB to about
30.6 MiB because checkpoints are less frequent.

Compile/dependency cleanup removed redundant direct `async-channel`,
`futures-util`, `thiserror`, and `nyquest-interface` declarations, plus unused
private protobuf wire variants. docs.rs now uses one valid explicit
backend/compression/persistence feature set instead of incompatible
all-features. Invalid dual-backend and dual-compression configurations produce
direct compile errors.

### Sampling-guided compiler hints

Release builds with debug symbols were sampled on macOS for eight seconds.
The 3,000,000-event, eight-producer persistence profile recorded about 82% of
samples in the SQLite writer. WAL writes and commit processing dominated; this
does not justify forcing Rust admission functions inline.

The 500,000,000-attempt memory-only profile concentrated application samples
in `Reporter::try_report`, the bounded queue, and `Log::with`. Three-run
100,000,000-attempt A/B tests showed that inlining `Log::with` raised median
throughput from 19.24 million to 20.44 million attempts/second (about 6%).
Inlining the much larger `Reporter::try_report` together with it reduced median
throughput to 16.88 million attempts/second, so that hint was rejected.
No `#[inline(always)]` annotations were added.

## Per-event upload flag and sent-row retention addendum (2026-10-19)

This addendum is the current state for `persist` builds and supersedes the
persistence addendum where it says that acknowledged rows are deleted, that
every spool row is queued for upload, or that retention only evicts unassigned
rows.

### Why

The spool now doubles as the application's local log history: every event is
stored, the application decides per event whether it is uploaded, and uploaded
rows stay on disk after SLS accepts them. Retention by age, count, and storage
is the only thing that removes rows. Previously every row was queued for
upload, acknowledgement deleted rows, and retention could only remove
unassigned (that is, not yet sent) rows.

### What changed

- Schema v5 adds `pending_events.upload` (default `1`) and
  `pending_events.sent` (default `0`). Every v4 row was queued and
  unacknowledged, so the defaults describe existing rows exactly; an assigned
  v4 batch keeps its sequence ID and spool key. `pending_events_batch` becomes
  a partial index over assigned/sent rows, and the new partial index
  `pending_events_unsent` covers unassigned upload rows, so batch selection
  does not scan sent or local-only rows. The upgrade runs in the same
  `IMMEDIATE` transaction as the `user_version` bump; a failed or interrupted
  upgrade leaves a v4 file that a later open upgrades again.
- `Reporter::try_report_with_upload` / `report_with_upload` take the upload
  flag. `try_report` / `report` are unchanged and mark rows for upload.
  `ReportResult::NotStored` reports a local-only event given to a reporter
  without a persistent spool (it is discarded, never uploaded).
- `tracing_aliyun_sls::Layer::with_upload_decision` installs a
  `Fn(&Event, &mut Log) -> bool` callback. The crate re-exports `Log` and
  `MayStaticKey` so callers can add fields.
- Batch selection takes only `upload = 1` rows that are unassigned (and
  therefore unsent). Acknowledgement runs `UPDATE ... SET sent = 1` and deletes
  the `assigned_batches` row in one transaction; it no longer deletes events.
- Retention evicts any row except the assigned, unacknowledged batch:
  `batch_seq IS NULL OR sent = 1`. Startup limit enforcement uses the same
  predicate.
- `persistence_abandoned` counts evicted rows with `upload = 1 AND sent = 0`.
  `dropped` now grows only by abandoned rows; evicting sent or local-only rows
  is ordinary retention and is visible only in `persistence_evicted_*` and
  `persistence_evicted_bytes`. `persistence_pending_rows` / `_bytes` now count
  every stored row. `persistence_recovered_rows` counts unsent upload rows
  found at open; only those rows are decoded at startup and marked recovered.
- The uploader is woken only when a committed group contains an upload row.

### Invariants that changed

- **Admission (`on_event`)**: unchanged for builds without `persist` and for
  layers without an upload decision. With a decision installed, the callback
  is application code on the `on_event` path. The library imposes the same
  rule on it as on itself: no HTTP, sleep, retry, blocking channel send, or
  re-entrant `tracing`. Persistent admission still synchronously waited for
  the SQLite commit at this point (superseded by the asynchronous-write
  addendum of 2026-10-20); a local-only row costs the same commit.
- **Acknowledged rows** are retained instead of deleted. Their removal is now
  a retention decision, and a full spool of sent rows is evicted oldest-first
  rather than blocking admission.
- **Retention scope** widened from unassigned rows to all rows except the
  assigned batch. The assigned-batch protection is unchanged.
- **`dropped`** no longer includes retention of rows that were already sent or
  never meant to be sent.

### Guarantees

- A row marked local-only is never assigned to a batch and never uploaded.
- A row whose acknowledgement committed is never sent again, including after a
  restart.
- The duplicate window is unchanged: from SLS accepting a batch to the local
  acknowledgement commit. A process killed inside it reopens the same assigned
  batch and resends it with the same spool key and `seqid`. The loss window is
  unchanged: from `on_event` entry to the writer's commit.
- Per-event write cost did not grow: measured WAL frames per committed upload
  row 4.44 before / 4.43 after; a local-only row writes 3.37 because it is not
  in either partial index. Assign plus acknowledge of a 100-row batch wrote
  36.6 frames before (DELETE) and 34.4 after (UPDATE). Method:
  `wal_frames_per_event` ignored test, release build, 400-byte payload,
  `wal_autocheckpoint=0`, 2,000 preloaded rows, one transaction per event.

### Not guaranteed

- Correctness of the per-event decision is the application's responsibility,
  including how it behaves under contention; a slow or blocking callback
  delays the emitting thread.
- A local-only event is discarded when the reporter has no persistent spool.
- Synthesized span lifecycle events have no `Event` and are always marked for
  upload.
- Retention can remove unsent upload rows (abandoned uploads, counted) exactly
  as before; the guarantees and non-guarantees of the persistence addendum
  about power loss, OS crash, and live SLS `seqid` deduplication still apply.

### Validation

Deterministic tests cover: v4 to v5 upgrade with an assigned batch keeping its
`seqid`; an interrupted upgrade rolling back to v4 and succeeding on retry;
local-only rows never batched; sent rows retained and not resent after reopen;
age, count, storage, and startup retention removing sent, local-only, and
unsent rows with only the unsent ones counted as abandoned; the assigned batch
surviving retention; per-row flags through `try_report_with_upload`;
`NotStored` without a spool; the Layer callback; and two subprocess `kill -9`
tests (killed after commit before send, killed after a fake SLS accepted the
batch but before local acknowledgement) that reopen the spool and check that a
recording fake sink receives the row with the expected `seqid` and spool key.
Mutations reverting acknowledgement to `DELETE`, dropping the `upload = 1`
batch filter, or restricting retention to unassigned rows each fail several of
these tests.

### Follow-up (2026-10-19)

- **Upload decision panics** are caught in `on_event` with `catch_unwind`; the
  event is then marked for upload, the same as without a decision. A panic in
  application code on the logging path therefore cannot unwind into the
  emitting caller, and the failure mode only uploads more. Propagating the
  panic was rejected because a log statement would then abort the caller's
  task or thread. `AssertUnwindSafe` is sound here because only `log`, which
  is valid in any partially edited state, is used after a caught panic. With
  `panic = "abort"` nothing is caught.
- **`persistence_unsent_rows`** (new, additive) is the upload backlog: rows
  with `upload = 1 AND sent = 0`, including the assigned batch. It is seeded
  from the rows recovered at open and updated by the writer thread from each
  applied outcome: `+1` per committed upload row, `-abandoned`, `-acknowledged`
  (rows acknowledgement changed from unsent to sent).
- **Counter relations** are documented on `ReporterStatsSnapshot::received`
  and `::dropped`: every admission call adds one to `received` and is either
  accepted, rejected (in `dropped`), or `NotStored` (in neither); `dropped`
  additionally counts accepted logs lost later.
- **Wake-up tests**: a committed group of local-only rows does not wake the
  uploader, a group containing an upload row does, and a waiting
  `next_persistent_batch` returns the new upload row.

## Asynchronous persistence writes and local-only spool addendum (2026-10-20)

This addendum is the current state for `persist` builds and supersedes every
earlier statement that persistent admission waits for the SQLite commit or
that `Accepted` means committed.

### Why

Persistent admission used to send the row to the writer thread and block on
its reply, without a timeout. An application measured the consequence: with
the spool locked by another connection (`BEGIN EXCLUSIVE`, for example an
operator's `sqlite3` shell), the thread emitting a log waited for SQLite's busy
handler and then for every statement of the group, and the application's
command path stalled for many seconds. The application chose to make local
writes fully asynchronous and to accept losing the last few events on a crash.
It also needs the local history when SLS is not configured at all, which
required a spool without an SLS client.

### What changed

- **Admission** (`Reporter::try_report`, `try_report_with_upload`, `report*`,
  and therefore `Layer::on_event`): `PersistenceSender::enqueue` does one
  nonblocking `try_send` of the row onto the bounded command queue and
  returns. `Accepted` is counted at that point. The `Insert` command no longer
  carries a reply channel.
- **Writer thread** counts each row's outcome itself: `persistence_committed`
  on commit; on any failure (serialization, storage budget, or a failed group
  transaction) the new `persistence_write_failed`, plus `persistence_failed`
  and `dropped`. A failed group fails all its rows, as before; the writer
  then keeps serving the queue.
- **`PersistenceConfig::busy_timeout`** (new, default 5 s) is set explicitly on
  the writer's connection instead of relying on the library default. Only the
  writer waits for it.
- **Queue full**: when the writer falls behind by `queue_capacity` rows,
  admission returns `PersistenceFailed` and counts `queue_full`,
  `persistence_failed`, and `dropped`, as before; it never waits for capacity.
  In persistent mode `queue_full` counts exactly this queue (the in-memory
  queue is not used), so no second counter was added.
- **`Command::Flush`** answers once every earlier command was handled (the
  queue is FIFO). Graceful shutdown first drains the writer queue with it,
  racing the existing shutdown deadline, then sends the remaining batches
  under the same deadline. A writer stuck on a lock therefore cannot hold up
  shutdown beyond `shutdown_timeout`.
- **`Reporter::local_only(ReporterConfig, PersistenceConfig)`** starts the
  writer without an SLS client. Rows keep their `upload` flag; nothing is ever
  assigned or sent. Its reporting future waits for the shutdown signal and
  drains the writer queue. `Reporter` and `Reporting` hold the client as an
  `Option`; only `local_only` has none, and it always has a spool.
- **Destination binding**: a spool created without a client stores an empty
  destination fingerprint. The first client that opens it writes its
  fingerprint, after which the existing exact-match check applies; its
  retained `upload = 1 AND sent = 0` rows are recovered and uploaded like any
  other. A local-only reporter accepts a spool already bound to a destination
  and leaves the binding unchanged.

### Invariants

- **Admission (`on_event`)**: in every mode, no HTTP, sleep, retry,
  compression, disk I/O, SQLite lock wait, or blocking channel send occurs in
  admission. This restores the original hardened invariant for `persist`
  builds. An upload decision callback is still application code on that path.
- **Idempotency and no-duplicate semantics are unchanged**: only
  `upload = 1`, unsent, unassigned rows form a batch; one assigned batch at a
  time with a stable spool key and monotonic `seqid`; local acknowledgement
  marks rows sent. Commands are still handled in FIFO order, so a `NextBatch`
  sees every insert queued before it.
- **Loss window widened**: previously from `on_event` entry to the commit,
  with the caller waiting through it; now the same span, but the caller has
  already returned. A crash or power loss loses rows still in the queue.
  The `NORMAL` durability statements apply only to committed rows.

### Not guaranteed

- Rows accepted but still queued when the process is killed are lost.
- A lock held longer than `busy_timeout` fails the group the writer is
  holding (counted); rows queued behind it wait, and are rejected at
  admission once the queue is full (counted).
- A real full disk was not simulated in tests; it reaches the writer as a
  SQLite error on the group transaction, the same path the forced group
  failure and the lock timeout exercise.
- Opening a bound spool with a different destination still fails with
  `DestinationMismatch`, as before.

### Validation

New deterministic tests, each shown to fail under a targeted mutation:

| Test | Mutation that turns it red |
|---|---|
| `locked_spool_does_not_block_admission_and_rows_follow_unlock`: 1000 admissions with the spool under `BEGIN EXCLUSIVE`, each must return within 50 ms; after unlock all 1000 rows are committed | admission waits for the writer (restoring synchronous semantics): first call took 5.2 s |
| `full_writer_queue_drops_and_counts`: queue of 4 under a lock, 20 admissions | not counting `queue_full` |
| `write_failure_under_lock_is_counted_and_writer_recovers`: `busy_timeout` 50 ms under a lock, 10 rows counted as write failures, a later row commits | not counting `persistence_write_failed` |
| `local_only_reporter_stores_without_client_and_later_client_uploads` | not binding an unbound spool (`DestinationMismatch`) |
| `graceful_shutdown_drains_the_writer_queue`: 100 rows queued behind a lock released after 200 ms are committed when shutdown returns | removing the shutdown drain (0 of 100 committed) |
| `graceful_shutdown_drain_is_bounded_by_the_shutdown_timeout`: 200 ms deadline, writer stuck on a 30 s lock | draining without the deadline (62 s) |

Measured `try_report` latency with the spool locked, 1000 calls, one run each
on an Apple Silicon Mac: debug p50 500 ns, p99 1.54 µs, max 12.25 µs; release
p50 83 ns, p99 1.17 µs, max 3.92 µs.

The persistent stress example now retries on a full writer queue and measures
until the last row is committed. Release, 100,000 events, one run each:
1 producer 224,041 events/s end to end (synchronous admission before:
35,824-36,700 events/s), per-attempt admission average 75 ns, max 29.5 µs;
8 producers 210,867 events/s (before: 95,304-96,291 events/s). The writer
now always finds full 32-row groups waiting, which explains most of the gain;
these are single local runs, not a performance guarantee.

Commands run on the final tree, all exit 0: `cargo fmt --all -- --check`;
`cargo clippy -- -D warnings` with `--all-targets` for the workspace, the
workspace with `persist`, `aliyun-sls` with `persist`, and `aliyun-sls` with
`--no-default-features --features reporter,reqwest-rustls,lz4`;
`cargo test --workspace` (22 `aliyun-sls` and 4 `tracing-aliyun-sls` unit
tests, 1 ignored); `cargo test --workspace --features persist` (78
`aliyun-sls` unit tests with 2 ignored, the 2 subprocess crash tests, 6
`tracing-aliyun-sls` unit tests); `cargo test --workspace --doc`.
