# Aliyun SLS Client

Client for [Aliyun SLS](https://help.aliyun.com/zh/sls/) (Aliyun Log Service).

## Feature Flags

- `reporter`: bounded batching, retry, shutdown, and reporter statistics.
- `persist`: SQLite-backed reporter admission; implies `reporter`, but also
  requires runtime opt-in with `ReporterBuilder::build_with_persistence`.

### Http backend

- [`reqwest`]:
  `reqwest` feature gate Use [`reqwest`] as the HTTP backend, but do not enable any TLS features.
  - `reqwest-default-tls`: use [`reqwest`] as the HTTP backend and default TLS provider.
  - `reqwest-rustls`: use [`reqwest`] as the HTTP backend and [`rustls`] TLS provider.
  - or, uou can configure the TLS provider by manually enabling feature gates in [`reqwest`].
- [`nyquest`]: A platform native HTTP client, provides smaller binary size.

  To use this, you need to register the http client provider in your application, see
  <https://docs.rs/nyquest-preset/latest/nyquest_preset/#quick-start> for more details:
  - [`nyquest-preset`]: default client configuration for [`nyquest`]
  - [`nyquest-backend-winrt`]: [`UWP/WinRT HttpClient`] for [`nyquest`]
  - [`nyquest-backend-curl`]: libcurl backend for [`nyquest`], requires libcurl _7.68.0_ or later.
  - [`nyquest-backend-nsurlsession`]: macOS/iOS [`NSURLSession`] backend for [`nyquest`].

### Compression

> Note: `lz4` and `deflate` cannot be enabled at the same time.

- `lz4`: enable lz4 compression for logs.
- `deflate`: enable deflate compression for logs.

### Inline Optimizations

Inline features can control how many key-pairs are inlined before spill over to the heap.
If multiple inline features are enabled, the maximum number of inlined key-pairs will be used.

By default, the client will inline up to 8 key-pairs for log messages and 8 key-pairs for log group metadata tags.
 
If `inline-none` feature is enabled, other inline features will be ignored, and no key-pairs will be inlined.

For log message key-pairs, use:
- `inline-keypairs-2`
- `inline-keypairs-4`
- `inline-keypairs-8` (default)
- `inline-keypairs-16`

For log group metadata tags, use:
- `inline-tags-2`
- `inline-tags-4`
- `inline-tags-8` (default)
- `inline-tags-16`

## Persistent reporter

```toml
aliyun-sls = { version = "0.1", features = ["persist", "reqwest-rustls"] }
```

```rust
use aliyun_sls::reporter::{PersistenceConfig, Reporter};
use std::{path::PathBuf, time::Duration};

# fn build(client: aliyun_sls::SlsClient) -> Result<Reporter, Box<dyn std::error::Error>> {
let persistence = PersistenceConfig::new(512 * 1024 * 1024)
    .path(PathBuf::from("/var/lib/my-service/sls-spool.sqlite3"))
    .max_events(1_000_000)
    .max_age(Duration::from_secs(7 * 24 * 60 * 60));
let reporter = Reporter::builder(client)
    .build_with_persistence(persistence)?;
# Ok(reporter)
# }
```

The feature and builder call are a double opt-in. `max_storage_bytes` is
required; `max_events` and `max_age` are optional. If no path is supplied, the
default is `<current-executable>.sls-spool.sqlite3` beside the executable. The
database file is mode `0600` on Unix.

The spool uses WAL, `synchronous=NORMAL`, and `auto_vacuum=NONE`. One dedicated
thread serializes all SQLite writes and one upload lane preserves sequence
order. Persistent `try_report` never waits for the network or queue capacity,
but does synchronously wait for the SQLite commit. A full/closed persistence
command queue, serialization/limit rejection, or SQLite error immediately
returns `ReportResult::PersistenceFailed`.

Retention evicts the oldest eligible unassigned rows by age, count, then
storage pressure; assigned rows cannot be evicted. The SQLite physical budget
is a conservative target, not an exact byte cap: WAL checkpoint timing,
filesystem allocation, and an assigned batch can make `.db` + `.wal` + `.shm`
larger. CPU includes serialization and SQLite work. Memory is bounded by the
command queue, one decoded upload batch, SQLite cache, and request buffers.
Admission adds synchronous disk I/O and is therefore not the memory-only
reporter's nonblocking/no-disk-I/O hot path.

A spool is bound to its endpoint/project/logstore. Each assigned batch keeps
the same spool key and monotonic `seqid`; retry deduplication relies on SLS
route-key/`seqid` behavior. Unavailable SLS, retry exhaustion, timeout,
shutdown deadline, and restart retain the batch. If remote acknowledgement
happens before local SQLite acknowledgement, restart resends the same batch
and `seqid`; only the local acknowledgement transaction deletes it. Graceful
shutdown drains until its configured deadline.

`Reporter::stats()` includes committed/failed persistent admissions,
count/age/storage evictions and evicted bytes, recovered/replayed rows, pending
rows/serialized bytes, and failed durable upload cycles. Stats are
process-local. Stored payloads include all log content, topic, source, and
tags; do not record secrets or sensitive data without treating the spool as
sensitive storage.

`NORMAL` provides an ordinary process-crash recovery guarantee for committed
rows, not a guarantee against power loss, OS crash, disk/controller/filesystem
failure, or faulty write caches.

The network-free stress harness declares the required feature, cleans its
temporary spool, and reports admission latency/throughput, Linux RSS when
available, pending rows/bytes, evictions/failures, and combined
`.db`/`.wal`/`.shm` size:

```text
cargo run -p aliyun-sls --features persist --example persistent_stress --release -- 100000
cargo run -p aliyun-sls --features persist --example persistent_stress --release -- 1000000
```

It does not start reporting or fake a successful delivery.

## Unstable Features

> Those features are unstable and requires a nightly build of the Rust toolchain.

- `may_dangle`: This feature makes the Rust compiler less strict about use of vectors that
  contain borrowed references. For details, see the
  [Rustonomicon](https://doc.rust-lang.org/1.42.0/nomicon/dropck.html#an-escape-hatch).

  Tracking issue: [rust-lang/rust#34761](https://github.com/rust-lang/rust/issues/31844)

[`reqwest`]: https://docs.rs/reqwest
[`rustls`]: https://docs.rs/rustls
[`nyquest`]: https://docs.rs/nyquest
[`nyquest-preset`]: https://docs.rs/nyquest-preset
[`nyquest-backend-nsurlsession`]: https://docs.rs/nyquest-backend-nsurlsession
[`UWP/WinRT HttpClient`]: https://learn.microsoft.com/en-us/uwp/api/Windows.Web.Http.HttpClient
[`NSURLSession`]: https://developer.apple.com/documentation/foundation/nsurlsession