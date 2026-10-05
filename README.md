# tracing-aliyun-sls
[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
![MIT licensed][license-badge]
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flightsing%2Ftracing-aliyun-sls.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Flightsing%2Ftracing-aliyun-sls?ref=badge_shield)

[crates-badge]: https://img.shields.io/crates/v/tracing-aliyun-sls.svg
[crates-url]: https://crates.io/crates/tracing-aliyun-sls
[docs-badge]: https://docs.rs/tracing-aliyun-sls/badge.svg
[docs-url]: https://docs.rs/tracing-aliyun-sls
[license-badge]: https://img.shields.io/badge/license-MIT%20OR%20Apache2.0-blue.svg

Send [`tracing`](https://docs.rs/tracing) events to
[Aliyun SLS](https://help.aliyun.com/zh/sls) through a bounded, asynchronous
reporting pipeline.

## Feature Flags

- `lz4`: enable LZ4 compression.
- `deflate`: enable deflate compression.
- `reqwest-default-tls`: use the reqwest backend with rustls (default).
- `reqwest-rustls`: use the reqwest backend with rustls explicitly.
- `nyquest`: use the platform-native nyquest backend.

Note: `lz4` and `deflate` cannot be enabled at the same time.

## Example

```rust
use std::time::Duration;
use tracing_aliyun_sls::{
    SlsClient,
    reporter::Reporter,
};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = SlsClient::builder()
        .access_key("access_key")
        .access_secret("access_secret")?
        .endpoint("cn-hangzhou.log.aliyuncs.com")
        .project("project")
        .logstore("logstore")
        .connect_timeout(Duration::from_secs(3))
        .request_timeout(Duration::from_secs(10))
        .enable_trace(false)
        .build()?;

    let reporter = Reporter::builder(client)
        .with_queue_capacity(65_536)
        .with_batch_max_count(4_096)
        .with_batch_max_bytes(1024 * 1024)
        .with_linger(Duration::from_secs(2))
        .with_linger_jitter_percent(20)
        .with_max_in_flight(2)
        .with_retry_max_attempts(5)
        .with_retry_base_delay(Duration::from_millis(200))
        .with_retry_max_delay(Duration::from_secs(10))
        .with_shutdown_timeout(Duration::from_secs(5))
        .build();

    let reporting = reporter
        .reporting_default()
        .await
        .expect("reporting can only be started once");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let reporting_task = tokio::spawn(
        reporting
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .start(),
    );

    let layer = tracing_aliyun_sls::layer(reporter.clone());
    tracing_subscriber::registry()
        .with(layer)
        .init();

    tracing::info!("application started");

    let _ = shutdown_tx.send(());
    let _ = reporting_task.await;
    Ok(())
}
```

## Production behavior

- Event admission uses a bounded queue and never waits for network or queue
  capacity. A full or closed queue drops the new event.
- Batches flush at 4,096 logs, 1 MiB estimated encoded size, or a 2 second
  linger interval with 20% jitter.
- Transient connect/timeout failures and HTTP 408, 429, 500, 502, 503, and 504
  retry up to 5 total attempts with capped exponential full jitter.
- HTTP 400, 401, 403, and 404 are permanent by default.
- At most 2 uploads run concurrently.
- Graceful shutdown drains accepted work for at most 5 seconds, then accounts
  remaining telemetry as dropped.
- `Reporter::stats()` returns an atomic point-in-time snapshot of admission,
  queue, retry, delivery, batch, and drop counters.

All limits above are configurable through `ReporterBuilder` or
`ReporterConfig`.

## Security

All tracing fields passed to this Layer may be uploaded to Aliyun SLS. Do not
record credentials, access secrets, authorization values, tokens, or other
sensitive data as tracing fields.

Load the access key and secret from protected application configuration. The
library does not include arbitrary-field secret detection because such
detection is unreliable.

Internal client diagnostics use the dedicated
`tracing_aliyun_sls_internal` target, which this SLS Layer excludes to prevent
recursive reporting. Diagnostics never include the authorization header,
access secret, or response body.

## Stress test

The stress example leaves delivery unavailable on purpose and measures the
bounded admission path:

```text
cargo run -p aliyun-sls --features reporter --example reporter_stress --release -- 100000
cargo run -p aliyun-sls --features reporter --example reporter_stress --release -- 1000000
```

## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flightsing%2Ftracing-aliyun-sls.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Flightsing%2Ftracing-aliyun-sls?ref=badge_large)