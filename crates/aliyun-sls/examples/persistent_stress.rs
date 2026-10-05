//! Measures SQLite-backed admission without starting the upload worker.
//!
//! No request is sent and no successful delivery is simulated:
//!
//! ```text
//! cargo run -p aliyun-sls --features persist --example persistent_stress --release -- 100000
//! cargo run -p aliyun-sls --features persist --example persistent_stress --release -- 100000 536870912 4
//! cargo run -p aliyun-sls --features persist --example persistent_stress --release -- 1000000
//! ```
//!
//! Arguments are event count, storage bytes, and producer thread count.

use aliyun_sls::{
    Log, LogGroupMetadata, MayStaticKey, SlsClient,
    reporter::{PersistenceConfig, ReportResult, Reporter},
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct TemporarySpool(PathBuf);

impl Drop for TemporarySpool {
    fn drop(&mut self) {
        for _ in 0..20 {
            if std::fs::remove_dir_all(&self.0).is_ok() || !self.0.exists() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        eprintln!(
            "warning: could not remove temporary spool directory {}",
            self.0.display()
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_count = argument(1)?.unwrap_or(1_000_000);
    let max_storage_bytes = u64::try_from(argument(2)?.unwrap_or(512 * 1024 * 1024))?;
    let producer_count = argument(3)?.unwrap_or(1).max(1);
    let directory = temporary_directory();
    std::fs::create_dir_all(&directory)?;
    let cleanup = TemporarySpool(directory.clone());
    let path = directory.join("spool.sqlite3");

    let client = SlsClient::builder()
        .access_key("stress-test")
        .access_secret("not-a-production-secret")?
        .endpoint("example.invalid")
        .project("stress")
        .logstore("persistent-stress")
        .enable_trace(false)
        .build()?;
    // The reporting future is deliberately never started, so this exercises
    // persistent admission and retention without network access.
    let reporter = Reporter::builder(client)
        .build_with_persistence(PersistenceConfig::new(max_storage_bytes).path(path.clone()))?;
    let metadata = Arc::new(LogGroupMetadata::new().with_topic("persistent-stress"));

    let barrier = Arc::new(Barrier::new(producer_count));
    let started = Instant::now();
    let mut threads = Vec::with_capacity(producer_count);
    for producer in 0..producer_count {
        let reporter = reporter.clone();
        let metadata = metadata.clone();
        let barrier = barrier.clone();
        let start = event_count.saturating_mul(producer) / producer_count;
        let end = event_count.saturating_mul(producer.saturating_add(1)) / producer_count;
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            let mut admission_nanos = 0u128;
            let mut max_admission = Duration::ZERO;
            let mut unexpected = None;
            for index in start..end {
                let log = Log::new(u32::try_from(index).unwrap_or(u32::MAX), None).with(
                    MayStaticKey::from_static("message"),
                    "persistent stress event",
                );
                let admission_started = Instant::now();
                let result = reporter.try_report(metadata.clone(), log);
                let latency = admission_started.elapsed();
                admission_nanos = admission_nanos.saturating_add(latency.as_nanos());
                max_admission = max_admission.max(latency);
                if !matches!(
                    result,
                    ReportResult::Accepted | ReportResult::PersistenceFailed
                ) {
                    unexpected = Some(result);
                    break;
                }
            }
            (admission_nanos, max_admission, unexpected)
        }));
    }
    let mut admission_nanos = 0u128;
    let mut max_admission = Duration::ZERO;
    for thread in threads {
        let (thread_nanos, thread_max, unexpected) =
            thread.join().map_err(|_| "producer thread panicked")?;
        admission_nanos = admission_nanos.saturating_add(thread_nanos);
        max_admission = max_admission.max(thread_max);
        if let Some(result) = unexpected {
            return Err(format!("unexpected persistent admission result: {result:?}").into());
        }
    }
    let elapsed = started.elapsed();
    let stats = reporter.stats();
    let physical_bytes = sqlite_physical_bytes(&path)?;
    let average_nanos = admission_nanos / event_count.max(1) as u128;
    let rss = resident_set_bytes()
        .map(|bytes| bytes.to_string())
        .unwrap_or_else(|| "unavailable".to_owned());

    println!(
        "events={event_count} producers={producer_count} accepted={} persistence_failures={} \
         evicted_count={} evicted_age={} evicted_storage={} evicted_bytes={} \
         pending_rows={} pending_bytes={} sqlite_physical_bytes={physical_bytes} \
         elapsed={elapsed:?} events_per_second={:.0} \
         admission_average_ns={average_nanos} admission_max={max_admission:?} rss_bytes={rss}",
        stats.accepted,
        stats.persistence_failed,
        stats.persistence_evicted_count,
        stats.persistence_evicted_age,
        stats.persistence_evicted_storage,
        stats.persistence_evicted_bytes,
        stats.persistence_pending_rows,
        stats.persistence_pending_bytes,
        event_count as f64 / elapsed.as_secs_f64(),
    );

    drop(reporter);
    drop(cleanup);
    Ok(())
}

fn argument(index: usize) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    std::env::args()
        .nth(index)
        .map(|value| value.parse().map_err(Into::into))
        .transpose()
}

fn temporary_directory() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!(
        "aliyun-sls-persistent-stress-{}-{unique}",
        std::process::id()
    ))
}

fn sqlite_physical_bytes(path: &Path) -> std::io::Result<u64> {
    ["", "-wal", "-shm"]
        .into_iter()
        .try_fold(0u64, |total, suffix| {
            let candidate = PathBuf::from(format!("{}{suffix}", path.display()));
            match std::fs::metadata(candidate) {
                Ok(metadata) => Ok(total.saturating_add(metadata.len())),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(total),
                Err(error) => Err(error),
            }
        })
}

#[cfg(target_os = "linux")]
fn resident_set_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    kib.checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn resident_set_bytes() -> Option<u64> {
    None
}
