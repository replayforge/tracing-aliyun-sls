//! Exercises the admission hot path while delivery is completely unavailable.
//!
//! Run 100,000 and 1,000,000 events respectively:
//!
//! ```text
//! cargo run -p aliyun-sls --features reporter --example reporter_stress --release -- 100000
//! cargo run -p aliyun-sls --features reporter --example reporter_stress --release -- 1000000
//! ```

use aliyun_sls::{
    Log, LogGroupMetadata, MayStaticKey, SlsClient,
    reporter::{ReportResult, Reporter},
};
use std::{sync::Arc, time::Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_count = std::env::args()
        .nth(1)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(1_000_000usize);
    let queue_capacity = std::env::args()
        .nth(2)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(65_536usize);

    // The reporting worker is deliberately not started. This models a fully
    // unavailable delivery subsystem and isolates admission-path behavior.
    let client = SlsClient::builder()
        .access_key("stress-test")
        .access_secret("not-a-production-secret")?
        .endpoint("example.invalid")
        .project("stress")
        .logstore("stress")
        .enable_trace(false)
        .build()?;
    let reporter = Reporter::builder(client)
        .with_queue_capacity(queue_capacity)
        .build();
    let metadata = Arc::new(LogGroupMetadata::new().with_topic("stress"));

    let started = Instant::now();
    let mut accepted = 0usize;
    for index in 0..event_count {
        let log =
            Log::new(index as u32, None).with(MayStaticKey::from_static("message"), "stress event");
        if reporter.try_report(metadata.clone(), log) == ReportResult::Accepted {
            accepted += 1;
        }
    }
    let elapsed = started.elapsed();
    let stats = reporter.stats();

    assert!(accepted <= queue_capacity);
    assert!(stats.queue_depth <= queue_capacity as u64);
    println!(
        "events={event_count} accepted={} dropped={} queue_depth={} elapsed={elapsed:?} events_per_second={:.0}",
        stats.accepted,
        stats.dropped,
        stats.queue_depth,
        event_count as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}
