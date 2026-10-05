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
producer -> bounded persistence command queue
         -> one SQLite writer (WAL / NORMAL / auto_vacuum NONE)
         -> pending row committed -> Accepted returned
         -> one assigned durable batch / one upload lane
         -> SLS route key + seqid retry
         -> local SQLite acknowledgement deletes assigned rows
```

The invariant changes deliberately: persistent admission still never waits for
network delivery or for queue capacity, but it synchronously waits for the
dedicated writer's SQLite transaction. It can therefore block for storage I/O
latency. A full/closed command queue, event/retention rejection, writer
failure, or SQLite error immediately returns `PersistenceFailed`; no
uncommitted event is reported as accepted.

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
Disk write latency is visible to persistent admission.

### Guarantees and non-guarantees

After `try_report` returns `Accepted` in persistent mode, its row's SQLite
transaction committed. With `synchronous=NORMAL`, this is intended to survive
ordinary process termination, including forced termination. A reopened
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
The workspace persist run passed 51 `aliyun-sls` unit tests, 2 persistence
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
