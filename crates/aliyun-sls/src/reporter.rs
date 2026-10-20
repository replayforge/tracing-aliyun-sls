//! A bounded, batching reporter for sending logs to the SLS service.
use crate::{
    Log, LogGroupMetadata, SlsClient, SlsClientError,
    proto::{calc_log_group_log_encoded_len, calc_log_group_metadata_encoded_len},
};
use async_channel::{Receiver, Sender, TrySendError};
use futures_timer::Delay;
use futures_util::{FutureExt, StreamExt, join, select, stream::FuturesUnordered};
use std::{
    collections::{HashMap, VecDeque},
    future::{Future, pending},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "persist")]
use crate::persistence::{AdmissionFailure, PersistenceSender, PersistentBatch};
#[cfg(feature = "persist")]
pub use crate::persistence::{PersistenceConfig, PersistenceError};

type Item = (Arc<LogGroupMetadata>, Log);
pub(crate) type Producer = Sender<Item>;
type Consumer = Receiver<Item>;
type SendFuture = Pin<Box<dyn Future<Output = SendOutcome> + Send>>;

trait LogSink: Clone + Send + Sync + 'static {
    fn send<'a>(
        &'a self,
        metadata: &'a LogGroupMetadata,
        logs: &'a [Log],
    ) -> Pin<Box<dyn Future<Output = Result<(), SlsClientError>> + Send + 'a>>;
}

#[cfg(feature = "persist")]
trait PersistentLogSink: Clone + Send + Sync + 'static {
    fn send_idempotent<'a>(
        &'a self,
        metadata: &'a LogGroupMetadata,
        logs: &'a [Log],
        spool_id: &'a [u8],
        sequence_id: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SlsClientError>> + Send + 'a>>;
}

const DEFAULT_QUEUE_CAPACITY: usize = 65_536;
const DEFAULT_BATCH_MAX_COUNT: usize = 4_096;
const DEFAULT_BATCH_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_LINGER: Duration = Duration::from_secs(2);
const DEFAULT_LINGER_JITTER_PERCENT: u8 = 20;
const DEFAULT_MAX_IN_FLIGHT: usize = 2;
const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 5;
const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(10);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_VEC_DEFAULT_CAPACITY: usize = 1024;
const VEC_POOL_DEFAULT_CAPACITY: usize = 1024;
const LOG_GROUP_DEFAULT_CAPACITY: usize = 1024;

/// Trait for creating a drain timer future.
pub trait DrainTimer: Send + Sync + 'static {
    /// Create a drain timer future.
    fn drain_timer(&self) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>;
}

/// Configuration for a [`Reporter`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReporterConfig {
    queue_capacity: usize,
    batch_max_count: usize,
    batch_max_bytes: usize,
    linger: Duration,
    linger_jitter_percent: u8,
    max_in_flight: usize,
    retry_max_attempts: usize,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
    shutdown_timeout: Duration,
}

/// Builder for a [`Reporter`].
pub struct ReporterBuilder {
    client: SlsClient,
    config: ReporterConfig,
}

/// Result of attempting to admit one log.
///
/// Without persistent mode, admission means enqueueing in memory. With a
/// reporter built by [`ReporterBuilder::build_with_persistence`] or
/// [`Reporter::local_only`], `Accepted` means the log was handed to the SQLite
/// writer thread; the commit happens afterwards and its outcome is counted in
/// `persistence_committed` or `persistence_write_failed`. A log accepted just
/// before the process dies may therefore not be on disk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReportResult {
    /// The log was accepted into memory or queued for the persistent spool.
    Accepted,
    /// The bounded queue had no available capacity.
    Full,
    /// The reporter has stopped accepting logs.
    Closed,
    /// The persistence command queue was full or closed, or the event exceeded
    /// the configured maximum batch size. SQLite failures happen after
    /// admission and are counted in `persistence_write_failed` instead.
    #[cfg(feature = "persist")]
    PersistenceFailed,
    /// The caller marked the log local-only, but this reporter has no
    /// persistent spool, so the log was neither stored nor uploaded.
    #[cfg(feature = "persist")]
    NotStored,
}

/// A point-in-time copy of reporter counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReporterStatsSnapshot {
    /// Logs presented to the reporter, including rejected logs.
    ///
    /// Every admission call (`try_report`, `report`, and with `persist`
    /// `try_report_with_upload`/`report_with_upload`) adds exactly one, and
    /// ends in exactly one of three ways:
    ///
    /// - accepted: counted in `accepted`;
    /// - rejected at admission (full, closed, or oversized in persistent
    ///   mode): counted in `dropped`;
    /// - `ReportResult::NotStored` (a local-only log without a persistent
    ///   spool): counted in neither.
    ///
    /// So `received >= accepted + admission rejections`, with equality when
    /// no call returned `NotStored`. `received` is not
    /// `accepted + dropped`: `dropped` also counts accepted logs that were lost
    /// later (see `dropped`).
    pub received: u64,
    /// Logs accepted into the in-memory queue or into the persistence command
    /// queue. In persistent mode this is counted before the SQLite write; see
    /// `persistence_committed` and `persistence_write_failed` for its outcome.
    pub accepted: u64,
    /// Logs rejected because the queue was full. In persistent mode this is the
    /// bounded queue in front of the SQLite writer thread, which fills while
    /// the writer is slow or waiting for a lock: these logs were neither
    /// stored nor uploaded.
    pub queue_full: u64,
    /// Logs rejected because the reporter was closed.
    pub queue_closed: u64,
    /// Logs successfully delivered.
    pub sent: u64,
    /// Retry attempts made after an initial send failure.
    pub retried: u64,
    /// Batches successfully delivered.
    pub batches_sent: u64,
    /// Batches permanently failed or abandoned at shutdown.
    pub batches_failed: u64,
    /// Logs whose send attempts were exhausted or classified as permanent.
    pub send_failed: u64,
    /// Logs larger than the configured maximum batch size.
    pub oversized: u64,
    /// Logs that will never reach SLS, counted once each:
    ///
    /// - rejected at admission (see `received`);
    /// - in-memory mode only: accepted but then found oversized when batched,
    ///   whose send attempts failed, or still queued or in flight at the
    ///   shutdown deadline;
    /// - persistent mode only: accepted logs the writer could not store
    ///   (`persistence_write_failed`), and rows marked for upload but not yet
    ///   sent when retention evicted them (`persistence_abandoned`).
    ///
    /// In persistent mode a failed send does not drop anything: the batch stays
    /// in the spool and is retried. Retention evicting a sent or local-only row
    /// is ordinary history cleanup and is not counted here, although it is in
    /// `persistence_evicted_*`. A `ReportResult::NotStored` log is not counted
    /// either: the caller asked not to upload it.
    pub dropped: u64,
    /// Logs currently waiting in the bounded in-memory or persistence command
    /// queue. Persistent rows already committed to SQLite are reported by
    /// `persistence_pending_rows`.
    pub queue_depth: u64,
    /// Logs the writer thread committed to the persistent spool.
    #[cfg(feature = "persist")]
    pub persistence_committed: u64,
    /// Logs lost on the way to the persistent spool: rejected at admission
    /// (queue full or closed, oversized) or accepted and then not written
    /// (`persistence_write_failed`).
    #[cfg(feature = "persist")]
    pub persistence_failed: u64,
    /// Accepted logs the writer thread could not store: the SQLite transaction
    /// failed (disk full, a lock held by another connection longer than
    /// [`PersistenceConfig::busy_timeout`], a damaged file) or the event did
    /// not fit the storage budget. The caller already got `Accepted`, so this
    /// counter is the only signal; it is also counted in `persistence_failed`
    /// and `dropped`.
    #[cfg(feature = "persist")]
    pub persistence_write_failed: u64,
    /// Rows evicted by the configured maximum event count. Like the other
    /// `persistence_evicted_*` counters this includes sent and local-only rows;
    /// see `persistence_abandoned` for evicted rows that were never uploaded.
    #[cfg(feature = "persist")]
    pub persistence_evicted_count: u64,
    /// Rows evicted by the configured maximum age.
    #[cfg(feature = "persist")]
    pub persistence_evicted_age: u64,
    /// Rows evicted to enforce the persistence storage capacity.
    #[cfg(feature = "persist")]
    pub persistence_evicted_storage: u64,
    /// Payload bytes removed while enforcing persistence limits.
    #[cfg(feature = "persist")]
    pub persistence_evicted_bytes: u64,
    /// Evicted rows that were marked for upload but had not been sent: their
    /// upload was abandoned. These are also counted in `dropped`; evicting a
    /// sent or local-only row is not.
    #[cfg(feature = "persist")]
    pub persistence_abandoned: u64,
    /// Unsent upload rows decoded and recovered when the persistent spool was
    /// opened.
    #[cfg(feature = "persist")]
    pub persistence_recovered_rows: u64,
    /// Rows currently stored in the persistent spool, including sent and
    /// local-only rows kept as local history. For the upload backlog see
    /// `persistence_unsent_rows`.
    #[cfg(feature = "persist")]
    pub persistence_pending_rows: u64,
    /// Serialized event bytes currently stored in the persistent spool,
    /// including sent and local-only rows.
    #[cfg(feature = "persist")]
    pub persistence_pending_bytes: u64,
    /// Rows currently in the persistent spool that are marked for upload and
    /// have not been acknowledged by SLS: the upload backlog, including the
    /// batch currently assigned for upload. Sent and local-only rows are not
    /// counted. It rises by one per committed upload row and falls when SLS
    /// acknowledges a batch or retention abandons an unsent row.
    #[cfg(feature = "persist")]
    pub persistence_unsent_rows: u64,
    /// Recovered logs delivered from a batch assigned before this process.
    #[cfg(feature = "persist")]
    pub persistence_replayed: u64,
    /// Persistent upload cycles that ended with the durable batch still pending.
    #[cfg(feature = "persist")]
    pub persistence_failed_cycles: u64,
}

/// A reporter for batching and sending logs to the SLS service.
#[derive(Clone)]
pub struct Reporter {
    state: Arc<State>,
    pub(crate) producer: Arc<Producer>,
    #[cfg(feature = "persist")]
    persistence: Option<Arc<PersistenceSender>>,
    consumer: Arc<Mutex<Option<Consumer>>>,
    /// `None` only for [`Reporter::local_only`], which always has a
    /// persistent spool.
    client: Option<SlsClient>,
    config: ReporterConfig,
}

/// A handle to the reporting process, allowing configuration and startup.
pub struct Reporting {
    state: Arc<State>,
    producer: Arc<Producer>,
    consumer: Consumer,
    client: Option<SlsClient>,
    config: ReporterConfig,
    #[cfg(feature = "persist")]
    persistence: Option<Arc<PersistenceSender>>,
    log_vec_capacity: usize,
    log_group_capacity: usize,
    vec_pool_capacity: usize,
    drain_timer: Box<dyn DrainTimer>,
    shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
}

#[derive(Default)]
pub(crate) struct Stats {
    pub(crate) received: AtomicU64,
    pub(crate) accepted: AtomicU64,
    pub(crate) queue_full: AtomicU64,
    pub(crate) queue_closed: AtomicU64,
    pub(crate) sent: AtomicU64,
    pub(crate) retried: AtomicU64,
    pub(crate) batches_sent: AtomicU64,
    pub(crate) batches_failed: AtomicU64,
    pub(crate) send_failed: AtomicU64,
    pub(crate) oversized: AtomicU64,
    pub(crate) dropped: AtomicU64,
    pub(crate) queue_depth: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_committed: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_failed: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_write_failed: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_evicted_count: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_evicted_age: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_evicted_storage: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_evicted_bytes: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_abandoned: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_recovered_rows: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_pending_rows: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_pending_bytes: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_unsent_rows: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_replayed: AtomicU64,
    #[cfg(feature = "persist")]
    pub(crate) persistence_failed_cycles: AtomicU64,
}

pub(crate) struct State {
    is_reporting: AtomicBool,
    is_closing: AtomicBool,
    pub(crate) stats: Stats,
}

struct PendingBatch {
    metadata: Arc<LogGroupMetadata>,
    logs: Vec<Log>,
    encoded_len: usize,
}

struct SendOutcome {
    log_count: usize,
    success: bool,
}

// Keeping the item inline avoids an extra allocation on every consumed log.
#[allow(clippy::large_enum_variant)]
enum WorkerEvent {
    Item(Result<Item, async_channel::RecvError>),
    Completed(SendOutcome),
    Drain,
    Shutdown,
}

struct BatchWorker<S> {
    consumer: Consumer,
    client: S,
    config: ReporterConfig,
    stats: Arc<State>,
    vec_pool: Vec<Vec<Log>>,
    current: HashMap<Arc<LogGroupMetadata>, PendingBatch>,
    ready: VecDeque<PendingBatch>,
    in_flight: FuturesUnordered<SendFuture>,
    in_flight_logs: usize,
    log_vec_capacity: usize,
    log_group_capacity: usize,
    vec_pool_capacity: usize,
}

impl LogSink for SlsClient {
    fn send<'a>(
        &'a self,
        metadata: &'a LogGroupMetadata,
        logs: &'a [Log],
    ) -> Pin<Box<dyn Future<Output = Result<(), SlsClientError>> + Send + 'a>> {
        Box::pin(self.try_put_log(metadata, logs))
    }
}

#[cfg(feature = "persist")]
impl PersistentLogSink for SlsClient {
    fn send_idempotent<'a>(
        &'a self,
        metadata: &'a LogGroupMetadata,
        logs: &'a [Log],
        spool_id: &'a [u8],
        sequence_id: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SlsClientError>> + Send + 'a>> {
        Box::pin(self.try_put_log_idempotent(metadata, logs, spool_id, sequence_id))
    }
}

impl Default for ReporterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            batch_max_count: DEFAULT_BATCH_MAX_COUNT,
            batch_max_bytes: DEFAULT_BATCH_MAX_BYTES,
            linger: DEFAULT_LINGER,
            linger_jitter_percent: DEFAULT_LINGER_JITTER_PERCENT,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            retry_max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
            retry_base_delay: DEFAULT_RETRY_BASE_DELAY,
            retry_max_delay: DEFAULT_RETRY_MAX_DELAY,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

impl ReporterConfig {
    /// Set the bounded admission queue capacity.
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity.max(1);
        self
    }

    /// Set the maximum logs in one batch.
    pub fn with_batch_max_count(mut self, count: usize) -> Self {
        self.batch_max_count = count.max(1);
        self
    }

    /// Set the maximum estimated protobuf bytes in one batch.
    pub fn with_batch_max_bytes(mut self, bytes: usize) -> Self {
        self.batch_max_bytes = bytes.max(1);
        self
    }

    /// Set the maximum time a partial batch waits before dispatch.
    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    /// Set symmetric linger jitter as a percentage, clamped to 100.
    pub fn with_linger_jitter_percent(mut self, percent: u8) -> Self {
        self.linger_jitter_percent = percent.min(100);
        self
    }

    /// Set the maximum number of concurrent requests.
    pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = max_in_flight.max(1);
        self
    }

    /// Set the maximum total attempts for each batch.
    pub fn with_retry_max_attempts(mut self, attempts: usize) -> Self {
        self.retry_max_attempts = attempts.max(1);
        self
    }

    /// Set the initial retry backoff cap.
    pub fn with_retry_base_delay(mut self, delay: Duration) -> Self {
        self.retry_base_delay = delay;
        self
    }

    /// Set the maximum retry backoff cap.
    pub fn with_retry_max_delay(mut self, delay: Duration) -> Self {
        self.retry_max_delay = delay;
        self
    }

    /// Set the graceful shutdown deadline.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }
}

impl ReporterBuilder {
    /// Create a builder for the given SLS client.
    pub fn new(client: SlsClient) -> Self {
        Self {
            client,
            config: ReporterConfig::default(),
        }
    }

    /// Replace the builder configuration.
    pub fn with_config(mut self, config: ReporterConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the bounded admission queue capacity.
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.config = self.config.with_queue_capacity(capacity);
        self
    }

    /// Set the maximum logs in one batch.
    pub fn with_batch_max_count(mut self, count: usize) -> Self {
        self.config = self.config.with_batch_max_count(count);
        self
    }

    /// Set the maximum estimated protobuf bytes in one batch.
    pub fn with_batch_max_bytes(mut self, bytes: usize) -> Self {
        self.config = self.config.with_batch_max_bytes(bytes);
        self
    }

    /// Set the partial-batch linger duration.
    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.config = self.config.with_linger(linger);
        self
    }

    /// Set symmetric linger jitter as a percentage.
    pub fn with_linger_jitter_percent(mut self, percent: u8) -> Self {
        self.config = self.config.with_linger_jitter_percent(percent);
        self
    }

    /// Set the maximum number of concurrent requests.
    pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.config = self.config.with_max_in_flight(max_in_flight);
        self
    }

    /// Set the maximum total attempts for each batch.
    pub fn with_retry_max_attempts(mut self, attempts: usize) -> Self {
        self.config = self.config.with_retry_max_attempts(attempts);
        self
    }

    /// Set the initial retry backoff cap.
    pub fn with_retry_base_delay(mut self, delay: Duration) -> Self {
        self.config = self.config.with_retry_base_delay(delay);
        self
    }

    /// Set the maximum retry backoff cap.
    pub fn with_retry_max_delay(mut self, delay: Duration) -> Self {
        self.config = self.config.with_retry_max_delay(delay);
        self
    }

    /// Set the graceful shutdown deadline.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_shutdown_timeout(timeout);
        self
    }

    /// Build the reporter.
    pub fn build(self) -> Reporter {
        let (producer, consumer) = async_channel::bounded(self.config.queue_capacity.max(1));
        Reporter {
            state: Arc::new(State::default()),
            producer: Arc::new(producer),
            #[cfg(feature = "persist")]
            persistence: None,
            consumer: Arc::new(Mutex::new(Some(consumer))),
            client: Some(self.client),
            config: self.config,
        }
    }

    /// Build a reporter that stores every admitted log in SQLite.
    ///
    /// This is the runtime opt-in required in addition to Cargo feature
    /// `persist`. The method starts one dedicated SQLite writer thread and
    /// validates that an existing spool belongs to this client's endpoint,
    /// project, and logstore. A spool created by [`Reporter::local_only`]
    /// is not bound to a destination yet; this binds it to this client and
    /// uploads its rows marked for upload.
    ///
    /// Admission only hands the log to the writer thread and never waits for
    /// the SQLite commit (see [`ReportResult`]).
    ///
    /// SQLite uses WAL mode, `synchronous=NORMAL`, and `auto_vacuum=NONE`.
    /// `NORMAL` protects committed rows from ordinary process termination, but
    /// does not guarantee survival across power loss, an operating-system
    /// crash, storage failure, or filesystem/hardware write-cache faults.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] for invalid limits, path or permission
    /// failures, incompatible/corrupt state, destination mismatch, SQLite
    /// setup failure, or failure to start the writer.
    #[cfg(feature = "persist")]
    #[cfg_attr(docsrs, doc(cfg(feature = "persist")))]
    pub fn build_with_persistence(
        self,
        persistence_config: PersistenceConfig,
    ) -> Result<Reporter, PersistenceError> {
        Reporter::start_persistent(Some(self.client), self.config, persistence_config)
    }
}

impl Reporter {
    /// Create a reporter builder with the given SLS client.
    pub fn builder(client: SlsClient) -> ReporterBuilder {
        ReporterBuilder::new(client)
    }

    /// Create a new reporter with the given SLS client.
    pub fn from_client(client: SlsClient) -> Self {
        ReporterBuilder::new(client).build()
    }

    /// Build a reporter that only keeps a local SQLite history, without an
    /// SLS client.
    ///
    /// Admission behaves as with [`ReporterBuilder::build_with_persistence`]
    /// and the `upload` flag is stored per row, but nothing is ever sent: no
    /// HTTP client exists. Run [`Reporter::reporting_default`] with a graceful
    /// shutdown as usual; in this mode it only drains the writer queue at
    /// shutdown. Of `config`, the queue capacity, maximum batch size (the
    /// admission size limit), and shutdown timeout apply.
    ///
    /// A new spool is not bound to a destination. When the application later
    /// starts with an SLS client on the same spool, that client binds it and
    /// uploads the rows that were marked for upload and are still retained.
    /// An existing spool bound to a destination is accepted as is.
    ///
    /// # Errors
    ///
    /// The same as [`ReporterBuilder::build_with_persistence`], except that
    /// there is no destination to mismatch.
    #[cfg(feature = "persist")]
    #[cfg_attr(docsrs, doc(cfg(feature = "persist")))]
    pub fn local_only(
        config: ReporterConfig,
        persistence_config: PersistenceConfig,
    ) -> Result<Self, PersistenceError> {
        Self::start_persistent(None, config, persistence_config)
    }

    #[cfg(feature = "persist")]
    fn start_persistent(
        client: Option<SlsClient>,
        config: ReporterConfig,
        persistence_config: PersistenceConfig,
    ) -> Result<Self, PersistenceError> {
        let (producer, consumer) = async_channel::bounded(config.queue_capacity.max(1));
        let state = Arc::new(State::default());
        let persistence = PersistenceSender::start(
            persistence_config,
            config.queue_capacity,
            client
                .as_ref()
                .map(SlsClient::persistence_destination_fingerprint),
            state.clone(),
        )?;
        Ok(Self {
            state,
            producer: Arc::new(producer),
            persistence: Some(persistence),
            consumer: Arc::new(Mutex::new(Some(consumer))),
            client,
            config,
        })
    }

    /// Create the reporting future with a given drain timer.
    ///
    /// If the reporter is already in reporting state, it returns `None`.
    pub async fn reporting(&self, drain_timer: impl DrainTimer) -> Option<Reporting> {
        if self.state.set_reporting() {
            return None;
        }
        let consumer = self.consumer.lock().ok()?.take()?;
        Some(self.make_reporting(consumer, Box::new(drain_timer)))
    }

    /// Create the reporting future using configured linger and jitter.
    ///
    /// If the reporter is already in reporting state, it returns `None`.
    pub async fn reporting_default(&self) -> Option<Reporting> {
        if self.state.set_reporting() {
            return None;
        }
        let consumer = self.consumer.lock().ok()?.take()?;
        let timer = LingerTimer {
            duration: self.config.linger,
            jitter_percent: self.config.linger_jitter_percent,
        };
        Some(self.make_reporting(consumer, Box::new(timer)))
    }

    fn make_reporting(&self, consumer: Consumer, drain_timer: Box<dyn DrainTimer>) -> Reporting {
        Reporting {
            state: self.state.clone(),
            producer: self.producer.clone(),
            consumer,
            client: self.client.clone(),
            config: self.config.clone(),
            #[cfg(feature = "persist")]
            persistence: self.persistence.clone(),
            log_vec_capacity: LOG_VEC_DEFAULT_CAPACITY,
            log_group_capacity: LOG_GROUP_DEFAULT_CAPACITY,
            vec_pool_capacity: VEC_POOL_DEFAULT_CAPACITY,
            drain_timer,
            shutdown_signal: Box::pin(pending()),
        }
    }

    /// Report a log, silently discarding it if admission fails.
    ///
    /// Use [`Reporter::try_report`] when the result must be observed.
    pub fn report(&self, metadata: Arc<LogGroupMetadata>, log: Log) {
        let _ = self.try_report(metadata, log);
    }

    /// Attempt to admit a log.
    ///
    /// This never blocks: in both modes it is a nonblocking bounded-channel
    /// `try_send`. In persistent mode the channel feeds the dedicated SQLite
    /// writer thread, and this call does not wait for the commit, a SQLite
    /// lock, or the network. A full or closed queue, or an event larger than
    /// the maximum batch size, returns [`ReportResult::PersistenceFailed`];
    /// a SQLite failure afterwards is only counted (`persistence_write_failed`).
    ///
    /// The log is marked for upload; this is the same as
    /// [`Reporter::try_report_with_upload`] with `upload = true`.
    pub fn try_report(&self, metadata: Arc<LogGroupMetadata>, log: Log) -> ReportResult {
        self.state.stats.received.fetch_add(1, Ordering::Relaxed);
        if let Some(closed) = self.reject_if_closing() {
            return closed;
        }
        #[cfg(feature = "persist")]
        if let Some(persistence) = &self.persistence {
            return self.admit_persistent(persistence, metadata, log, true);
        }
        match self.producer.try_send((metadata, log)) {
            Ok(()) => {
                self.state.stats.accepted.fetch_add(1, Ordering::Relaxed);
                self.state.stats.queue_depth.fetch_add(1, Ordering::Relaxed);
                ReportResult::Accepted
            }
            Err(TrySendError::Full(_)) => {
                self.state.stats.queue_full.fetch_add(1, Ordering::Relaxed);
                self.state.stats.dropped.fetch_add(1, Ordering::Relaxed);
                ReportResult::Full
            }
            Err(TrySendError::Closed(_)) => {
                self.state
                    .stats
                    .queue_closed
                    .fetch_add(1, Ordering::Relaxed);
                self.state.stats.dropped.fetch_add(1, Ordering::Relaxed);
                ReportResult::Closed
            }
        }
    }

    /// Report a log with a per-event upload decision, discarding the result.
    ///
    /// Use [`Reporter::try_report_with_upload`] when the result must be
    /// observed.
    #[cfg(feature = "persist")]
    #[cfg_attr(docsrs, doc(cfg(feature = "persist")))]
    pub fn report_with_upload(&self, metadata: Arc<LogGroupMetadata>, log: Log, upload: bool) {
        let _ = self.try_report_with_upload(metadata, log, upload);
    }

    /// Attempt to admit a log with a per-event upload decision.
    ///
    /// `upload = true` behaves exactly like [`Reporter::try_report`].
    ///
    /// `upload = false` stores the log in the persistent spool as local
    /// history only: it is written like any other row and removed by the
    /// configured retention, but it is never assigned to an upload batch. On a
    /// reporter without a persistent spool there is nowhere to keep such a log,
    /// so it is discarded and [`ReportResult::NotStored`] is returned.
    #[cfg(feature = "persist")]
    #[cfg_attr(docsrs, doc(cfg(feature = "persist")))]
    pub fn try_report_with_upload(
        &self,
        metadata: Arc<LogGroupMetadata>,
        log: Log,
        upload: bool,
    ) -> ReportResult {
        if upload {
            return self.try_report(metadata, log);
        }
        self.state.stats.received.fetch_add(1, Ordering::Relaxed);
        if let Some(closed) = self.reject_if_closing() {
            return closed;
        }
        match &self.persistence {
            Some(persistence) => self.admit_persistent(persistence, metadata, log, false),
            None => ReportResult::NotStored,
        }
    }

    fn reject_if_closing(&self) -> Option<ReportResult> {
        if !self.state.is_closing() {
            return None;
        }
        self.state
            .stats
            .queue_closed
            .fetch_add(1, Ordering::Relaxed);
        self.state.stats.dropped.fetch_add(1, Ordering::Relaxed);
        Some(ReportResult::Closed)
    }

    #[cfg(feature = "persist")]
    fn admit_persistent(
        &self,
        persistence: &PersistenceSender,
        metadata: Arc<LogGroupMetadata>,
        log: Log,
        upload: bool,
    ) -> ReportResult {
        if calc_log_group_metadata_encoded_len(&metadata)
            .saturating_add(calc_log_group_log_encoded_len(&log))
            > self.config.batch_max_bytes
        {
            self.state.stats.oversized.fetch_add(1, Ordering::Relaxed);
            self.state
                .stats
                .persistence_failed
                .fetch_add(1, Ordering::Relaxed);
            self.state.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return ReportResult::PersistenceFailed;
        }
        match persistence.enqueue(metadata, log, upload, &self.state) {
            Ok(()) => {
                self.state.stats.accepted.fetch_add(1, Ordering::Relaxed);
                ReportResult::Accepted
            }
            Err(failure) => {
                match failure {
                    AdmissionFailure::Full => {
                        self.state.stats.queue_full.fetch_add(1, Ordering::Relaxed);
                    }
                    AdmissionFailure::Closed => {
                        self.state
                            .stats
                            .queue_closed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                self.state
                    .stats
                    .persistence_failed
                    .fetch_add(1, Ordering::Relaxed);
                self.state.stats.dropped.fetch_add(1, Ordering::Relaxed);
                ReportResult::PersistenceFailed
            }
        }
    }

    /// Return a point-in-time snapshot of process-local reporter counters.
    ///
    /// Persistent snapshots include committed rows, failed admission and
    /// writes, retention
    /// evictions and bytes, recovered/replayed rows, current pending rows and
    /// serialized bytes, and upload cycles that left a batch durable.
    pub fn stats(&self) -> ReporterStatsSnapshot {
        self.state.snapshot()
    }

    /// Return a point-in-time snapshot of reporter counters.
    pub fn snapshot(&self) -> ReporterStatsSnapshot {
        self.state.snapshot()
    }
}

impl Reporting {
    /// Quit when `shutdown_signal` resolves.
    pub fn with_graceful_shutdown(
        mut self,
        shutdown_signal: impl Future<Output = ()> + Send + Sync + 'static,
    ) -> Self {
        self.shutdown_signal = Box::pin(shutdown_signal);
        self
    }

    /// Set the initial batching log vector capacity.
    pub fn with_log_vec_capacity(mut self, capacity: usize) -> Self {
        self.log_vec_capacity = capacity;
        self
    }

    /// Set the initial log group capacity.
    pub fn with_log_group_capacity(mut self, capacity: usize) -> Self {
        self.log_group_capacity = capacity;
        self
    }

    /// Set the initial internal vector pool capacity.
    pub fn with_vec_pool_capacity(mut self, capacity: usize) -> Self {
        self.vec_pool_capacity = capacity;
        self
    }

    /// Start the reporting process.
    pub async fn start(self) {
        let (shutdown_tx, shutdown_rx) = async_channel::bounded::<()>(1);
        let Reporting {
            state,
            producer,
            consumer,
            client,
            config,
            #[cfg(feature = "persist")]
            persistence,
            drain_timer,
            shutdown_signal,
            log_vec_capacity,
            log_group_capacity,
            vec_pool_capacity,
        } = self;

        #[cfg(feature = "persist")]
        if let Some(persistence) = persistence {
            let worker_state = state.clone();
            let work_fut = async move {
                match client {
                    Some(client) => {
                        persistent_worker(persistence, client, config, worker_state, shutdown_rx)
                            .await;
                    }
                    // Local-only: nothing to upload, so only wait for the
                    // shutdown and then drain the writer queue.
                    None => {
                        let _ = shutdown_rx.recv().await;
                        persistent_graceful_shutdown(persistence, None, config, worker_state).await;
                    }
                }
            };
            let shutdown_fut = async move {
                shutdown_signal.await;
                state.begin_close();
                producer.close();
                let _ = shutdown_tx.try_send(());
            };
            join!(work_fut, shutdown_fut);
            return;
        }

        // Only `Reporter::local_only` has no client, and it always has a
        // persistent spool, so the memory-only path always has one.
        let Some(client) = client else {
            return;
        };
        let mut vec_pool = Vec::with_capacity(vec_pool_capacity);
        vec_pool.resize_with(vec_pool_capacity, || Vec::with_capacity(log_vec_capacity));
        let mut worker = BatchWorker {
            consumer,
            client,
            config: config.clone(),
            stats: state.clone(),
            vec_pool,
            current: HashMap::with_capacity(log_group_capacity),
            ready: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            in_flight_logs: 0,
            log_vec_capacity,
            log_group_capacity,
            vec_pool_capacity,
        };

        let work_fut = async move {
            worker.run(drain_timer, shutdown_rx).await;
        };
        let shutdown_fut = async move {
            shutdown_signal.await;
            state.begin_close();
            producer.close();
            let _ = shutdown_tx.try_send(());
        };
        join!(work_fut, shutdown_fut);
    }
}

#[cfg(feature = "persist")]
async fn persistent_worker(
    persistence: Arc<PersistenceSender>,
    client: SlsClient,
    config: ReporterConfig,
    state: Arc<State>,
    shutdown: Receiver<()>,
) {
    loop {
        if shutdown.try_recv().is_ok() {
            break;
        }
        let batch = match next_persistent_batch(&persistence, &config, &shutdown).await {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(_) => {
                state
                    .stats
                    .persistence_failed_cycles
                    .fetch_add(1, Ordering::Relaxed);
                let wait = sleep(persistent_cycle_delay(&config)).fuse();
                let stop = shutdown.recv().fuse();
                futures_util::pin_mut!(wait, stop);
                if matches!(
                    futures_util::future::select(wait, stop).await,
                    futures_util::future::Either::Right(_)
                ) {
                    break;
                }
                continue;
            }
        };
        let send = persistent_send_cycle(&client, &batch, &config, &state).fuse();
        let stop = shutdown.recv().fuse();
        futures_util::pin_mut!(send, stop);
        match futures_util::future::select(send, stop).await {
            futures_util::future::Either::Left((success, _)) => {
                let finish = finish_persistent_cycle(&persistence, &batch, success, &state).fuse();
                let stop = shutdown.recv().fuse();
                futures_util::pin_mut!(finish, stop);
                if matches!(
                    futures_util::future::select(finish, stop).await,
                    futures_util::future::Either::Right(_)
                ) {
                    break;
                }
                if !success {
                    let wait = sleep(persistent_cycle_delay(&config)).fuse();
                    let stop = shutdown.recv().fuse();
                    futures_util::pin_mut!(wait, stop);
                    if matches!(
                        futures_util::future::select(wait, stop).await,
                        futures_util::future::Either::Right(_)
                    ) {
                        break;
                    }
                }
            }
            futures_util::future::Either::Right(_) => break,
        }
    }
    persistent_graceful_shutdown(persistence, Some(client), config, state).await;
}

#[cfg(feature = "persist")]
async fn next_persistent_batch(
    persistence: &PersistenceSender,
    config: &ReporterConfig,
    shutdown: &Receiver<()>,
) -> Result<Option<PersistentBatch>, PersistenceError> {
    let next = persistence
        .next_batch(config.batch_max_count, config.batch_max_bytes, false)
        .fuse();
    let stop = shutdown.recv().fuse();
    futures_util::pin_mut!(next, stop);
    let initial = match futures_util::future::select(next, stop).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => return Ok(None),
    };
    if let Some(batch) = initial {
        return Ok(Some(batch));
    }
    let notified = persistence.notified().fuse();
    let stop = shutdown.recv().fuse();
    futures_util::pin_mut!(notified, stop);
    if matches!(
        futures_util::future::select(notified, stop).await,
        futures_util::future::Either::Right(_)
    ) {
        return Ok(None);
    }

    let linger = sleep(jittered_linger_duration(
        config.linger,
        config.linger_jitter_percent,
    ))
    .fuse();
    futures_util::pin_mut!(linger);
    loop {
        let next = persistence
            .next_batch(config.batch_max_count, config.batch_max_bytes, false)
            .fuse();
        let stop = shutdown.recv().fuse();
        futures_util::pin_mut!(next, stop);
        let candidate = match futures_util::future::select(next, stop).await {
            futures_util::future::Either::Left((result, _)) => result?,
            futures_util::future::Either::Right(_) => return Ok(None),
        };
        if let Some(batch) = candidate {
            return Ok(Some(batch));
        }
        let notified = persistence.notified().fuse();
        let stop = shutdown.recv().fuse();
        futures_util::pin_mut!(notified, stop);
        select! {
            _ = notified => continue,
            _ = stop => return Ok(None),
            _ = linger => {
                let next = persistence
                    .next_batch(config.batch_max_count, config.batch_max_bytes, true)
                    .fuse();
                let stop = shutdown.recv().fuse();
                futures_util::pin_mut!(next, stop);
                return match futures_util::future::select(next, stop).await {
                    futures_util::future::Either::Left((result, _)) => result,
                    futures_util::future::Either::Right(_) => Ok(None),
                };
            }
        }
    }
}

#[cfg(feature = "persist")]
async fn persistent_send_cycle<S: PersistentLogSink>(
    client: &S,
    batch: &PersistentBatch,
    config: &ReporterConfig,
    state: &State,
) -> bool {
    for attempt in 1..=config.retry_max_attempts {
        match client
            .send_idempotent(
                &batch.metadata,
                &batch.logs,
                &batch.spool_id,
                batch.sequence_id,
            )
            .await
        {
            Ok(()) => return true,
            Err(error) if attempt < config.retry_max_attempts && is_retryable(&error) => {
                state.stats.retried.fetch_add(1, Ordering::Relaxed);
                sleep(retry_delay(config, attempt)).await;
            }
            Err(_) => break,
        }
    }
    false
}

#[cfg(feature = "persist")]
async fn finish_persistent_cycle(
    persistence: &PersistenceSender,
    batch: &PersistentBatch,
    success: bool,
    state: &State,
) {
    if success && persistence.acknowledge(batch.sequence_id).await.is_ok() {
        let count = batch.logs.len() as u64;
        state.stats.sent.fetch_add(count, Ordering::Relaxed);
        state.stats.batches_sent.fetch_add(1, Ordering::Relaxed);
        if batch.recovered_count != 0 {
            state
                .stats
                .persistence_replayed
                .fetch_add(batch.recovered_count as u64, Ordering::Relaxed);
        }
        return;
    }
    state.stats.batches_failed.fetch_add(1, Ordering::Relaxed);
    if !success {
        state
            .stats
            .send_failed
            .fetch_add(batch.logs.len() as u64, Ordering::Relaxed);
    }
    state
        .stats
        .persistence_failed_cycles
        .fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "persist")]
async fn persistent_graceful_shutdown(
    persistence: Arc<PersistenceSender>,
    client: Option<SlsClient>,
    config: ReporterConfig,
    state: Arc<State>,
) {
    let deadline = sleep(config.shutdown_timeout).fuse();
    futures_util::pin_mut!(deadline);
    // Drain the writer queue first, so logs admitted before shutdown are on
    // disk and can be part of the final batches. The same deadline bounds it:
    // a writer stuck on a SQLite lock cannot hold up shutdown.
    let flush = persistence.flush().fuse();
    futures_util::pin_mut!(flush);
    select! {
        _ = flush => {},
        _ = deadline => return,
    }
    let Some(client) = client else {
        return;
    };
    loop {
        let next = persistence
            .next_batch(config.batch_max_count, config.batch_max_bytes, true)
            .fuse();
        futures_util::pin_mut!(next);
        let batch = match select! {
            result = next => result.ok().flatten(),
            _ = deadline => None,
        } {
            Some(batch) => batch,
            None => return,
        };
        let send = persistent_send_cycle(&client, &batch, &config, &state).fuse();
        futures_util::pin_mut!(send);
        let success = select! {
            result = send => result,
            _ = deadline => return,
        };
        let finish = finish_persistent_cycle(&persistence, &batch, success, &state).fuse();
        futures_util::pin_mut!(finish);
        select! {
            _ = finish => {},
            _ = deadline => return,
        }
        if !success {
            let delay = sleep(persistent_cycle_delay(&config)).fuse();
            futures_util::pin_mut!(delay);
            select! {
                _ = delay => {},
                _ = deadline => return,
            }
        }
    }
}

#[cfg(feature = "persist")]
fn persistent_cycle_delay(config: &ReporterConfig) -> Duration {
    config
        .retry_max_delay
        .max(config.linger)
        .max(Duration::from_millis(100))
}

impl<S: LogSink> BatchWorker<S> {
    async fn run(&mut self, drain_timer: Box<dyn DrainTimer>, shutdown: Receiver<()>) {
        let mut drain_fut = drain_timer.drain_timer().fuse();
        loop {
            self.dispatch_ready();
            let event = if self.ready.is_empty() {
                {
                    let recv = self.consumer.recv().fuse();
                    let completion = wait_for_completion(&mut self.in_flight).fuse();
                    let shutdown_recv = shutdown.recv().fuse();
                    futures_util::pin_mut!(recv, completion, shutdown_recv);
                    select! {
                        item = recv => WorkerEvent::Item(item),
                        outcome = completion => WorkerEvent::Completed(outcome),
                        _ = drain_fut => WorkerEvent::Drain,
                        _ = shutdown_recv => WorkerEvent::Shutdown,
                    }
                }
            } else {
                {
                    let completion = wait_for_completion(&mut self.in_flight).fuse();
                    let shutdown_recv = shutdown.recv().fuse();
                    futures_util::pin_mut!(completion, shutdown_recv);
                    select! {
                        outcome = completion => WorkerEvent::Completed(outcome),
                        _ = shutdown_recv => WorkerEvent::Shutdown,
                        _ = drain_fut => WorkerEvent::Drain,
                    }
                }
            };
            match event {
                WorkerEvent::Item(Ok(item)) => self.push(item),
                WorkerEvent::Item(Err(_)) => {
                    self.stats.begin_close();
                    break;
                }
                WorkerEvent::Completed(outcome) => self.finish_send(outcome),
                WorkerEvent::Drain => {
                    self.flush_current();
                    drain_fut = drain_timer.drain_timer().fuse();
                }
                WorkerEvent::Shutdown => break,
            }
        }
        self.graceful_shutdown().await;
    }

    fn push(&mut self, (metadata, log): Item) {
        let _ = self.stats.stats.queue_depth.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |depth| Some(depth.saturating_sub(1)),
        );
        let added_len = calc_log_group_log_encoded_len(&log);
        if calc_log_group_metadata_encoded_len(&metadata).saturating_add(added_len)
            > self.config.batch_max_bytes
        {
            self.stats.stats.oversized.fetch_add(1, Ordering::Relaxed);
            self.stats.stats.send_failed.fetch_add(1, Ordering::Relaxed);
            self.stats.stats.dropped.fetch_add(1, Ordering::Relaxed);
            self.stats
                .stats
                .batches_failed
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let should_flush = self.current.get(&metadata).is_some_and(|batch| {
            !batch.logs.is_empty()
                && (batch.logs.len() >= self.config.batch_max_count
                    || batch.encoded_len.saturating_add(added_len) > self.config.batch_max_bytes)
        });
        if should_flush {
            if let Some(batch) = self.current.remove(&metadata) {
                self.ready.push_back(batch);
            }
        }

        let batch = self.current.entry(metadata.clone()).or_insert_with(|| {
            let logs = self
                .vec_pool
                .pop()
                .unwrap_or_else(|| Vec::with_capacity(self.log_vec_capacity));
            PendingBatch {
                encoded_len: calc_log_group_metadata_encoded_len(&metadata),
                metadata: metadata.clone(),
                logs,
            }
        });
        batch.encoded_len = batch.encoded_len.saturating_add(added_len);
        batch.logs.push(log);
        if batch.logs.len() >= self.config.batch_max_count
            || batch.encoded_len >= self.config.batch_max_bytes
        {
            if let Some(batch) = self.current.remove(&metadata) {
                self.ready.push_back(batch);
            }
        }
    }

    fn flush_current(&mut self) {
        self.ready
            .extend(self.current.drain().map(|(_, batch)| batch));
        self.current.shrink_to(self.log_group_capacity);
    }

    fn dispatch_ready(&mut self) {
        while self.in_flight.len() < self.config.max_in_flight {
            let Some(batch) = self.ready.pop_front() else {
                break;
            };
            self.in_flight_logs = self.in_flight_logs.saturating_add(batch.logs.len());
            let client = self.client.clone();
            let config = self.config.clone();
            let stats = self.stats.clone();
            self.in_flight.push(Box::pin(async move {
                send_batch(client, batch, config, stats).await
            }));
        }
    }

    fn finish_send(&mut self, outcome: SendOutcome) {
        self.in_flight_logs = self.in_flight_logs.saturating_sub(outcome.log_count);
        let count = outcome.log_count as u64;
        if outcome.success {
            self.stats.stats.sent.fetch_add(count, Ordering::Relaxed);
            self.stats
                .stats
                .batches_sent
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .stats
                .send_failed
                .fetch_add(count, Ordering::Relaxed);
            self.stats.stats.dropped.fetch_add(count, Ordering::Relaxed);
            self.stats
                .stats
                .batches_failed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn graceful_shutdown(&mut self) {
        while let Ok(item) = self.consumer.try_recv() {
            self.push(item);
        }
        self.flush_current();
        let deadline = sleep(self.config.shutdown_timeout).fuse();
        futures_util::pin_mut!(deadline);
        loop {
            self.dispatch_ready();
            if self.ready.is_empty() && self.in_flight.is_empty() {
                break;
            }
            let outcome = {
                let completion = wait_for_completion(&mut self.in_flight).fuse();
                futures_util::pin_mut!(completion);
                select! {
                    outcome = completion => Some(outcome),
                    _ = deadline => None,
                }
            };
            match outcome {
                Some(outcome) => self.finish_send(outcome),
                None => {
                    let ready_logs = self
                        .ready
                        .iter()
                        .map(|batch| batch.logs.len())
                        .sum::<usize>();
                    let dropped = ready_logs.saturating_add(self.in_flight_logs) as u64;
                    self.stats
                        .stats
                        .dropped
                        .fetch_add(dropped, Ordering::Relaxed);
                    self.stats
                        .stats
                        .send_failed
                        .fetch_add(dropped, Ordering::Relaxed);
                    let failed_batches =
                        self.ready.len().saturating_add(self.in_flight.len()) as u64;
                    self.stats
                        .stats
                        .batches_failed
                        .fetch_add(failed_batches, Ordering::Relaxed);
                    break;
                }
            }
        }
        self.ready.clear();
        self.in_flight = FuturesUnordered::new();
        self.vec_pool.truncate(self.vec_pool_capacity);
    }
}

async fn send_batch<S: LogSink>(
    client: S,
    batch: PendingBatch,
    config: ReporterConfig,
    state: Arc<State>,
) -> SendOutcome {
    let log_count = batch.logs.len();
    for attempt in 1..=config.retry_max_attempts {
        match client.send(&batch.metadata, &batch.logs).await {
            Ok(()) => {
                return SendOutcome {
                    log_count,
                    success: true,
                };
            }
            Err(error) if attempt < config.retry_max_attempts && is_retryable(&error) => {
                state.stats.retried.fetch_add(1, Ordering::Relaxed);
                sleep(retry_delay(&config, attempt)).await;
            }
            Err(_) => break,
        }
    }
    SendOutcome {
        log_count,
        success: false,
    }
}

fn is_retryable(error: &SlsClientError) -> bool {
    error.is_retryable()
}

fn retry_delay(config: &ReporterConfig, attempt: usize) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31) as u32;
    let multiplier = 1u128 << exponent;
    let cap = config
        .retry_base_delay
        .as_nanos()
        .saturating_mul(multiplier)
        .min(config.retry_max_delay.as_nanos());
    random_duration_up_to(cap)
}

async fn wait_for_completion(in_flight: &mut FuturesUnordered<SendFuture>) -> SendOutcome {
    match in_flight.next().await {
        Some(outcome) => outcome,
        None => pending().await,
    }
}

impl Default for State {
    fn default() -> Self {
        Self {
            is_reporting: AtomicBool::new(false),
            is_closing: AtomicBool::new(false),
            stats: Stats::default(),
        }
    }
}

impl State {
    fn set_reporting(&self) -> bool {
        self.is_reporting.swap(true, Ordering::AcqRel)
    }

    fn is_closing(&self) -> bool {
        self.is_closing.load(Ordering::Acquire)
    }

    fn begin_close(&self) {
        self.is_closing.store(true, Ordering::Release);
    }

    pub(crate) fn snapshot(&self) -> ReporterStatsSnapshot {
        ReporterStatsSnapshot {
            received: self.stats.received.load(Ordering::Relaxed),
            accepted: self.stats.accepted.load(Ordering::Relaxed),
            queue_full: self.stats.queue_full.load(Ordering::Relaxed),
            queue_closed: self.stats.queue_closed.load(Ordering::Relaxed),
            sent: self.stats.sent.load(Ordering::Relaxed),
            retried: self.stats.retried.load(Ordering::Relaxed),
            batches_sent: self.stats.batches_sent.load(Ordering::Relaxed),
            batches_failed: self.stats.batches_failed.load(Ordering::Relaxed),
            send_failed: self.stats.send_failed.load(Ordering::Relaxed),
            oversized: self.stats.oversized.load(Ordering::Relaxed),
            dropped: self.stats.dropped.load(Ordering::Relaxed),
            queue_depth: self.stats.queue_depth.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_committed: self.stats.persistence_committed.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_failed: self.stats.persistence_failed.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_write_failed: self.stats.persistence_write_failed.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_evicted_count: self.stats.persistence_evicted_count.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_evicted_age: self.stats.persistence_evicted_age.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_evicted_storage: self
                .stats
                .persistence_evicted_storage
                .load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_evicted_bytes: self.stats.persistence_evicted_bytes.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_abandoned: self.stats.persistence_abandoned.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_recovered_rows: self
                .stats
                .persistence_recovered_rows
                .load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_pending_rows: self.stats.persistence_pending_rows.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_pending_bytes: self.stats.persistence_pending_bytes.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_unsent_rows: self.stats.persistence_unsent_rows.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_replayed: self.stats.persistence_replayed.load(Ordering::Relaxed),
            #[cfg(feature = "persist")]
            persistence_failed_cycles: self.stats.persistence_failed_cycles.load(Ordering::Relaxed),
        }
    }
}

impl<F, Fut> DrainTimer for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + Sync + 'static,
{
    fn drain_timer(&self) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(self())
    }
}

struct LingerTimer {
    duration: Duration,
    jitter_percent: u8,
}

impl DrainTimer for LingerTimer {
    fn drain_timer(&self) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(sleep(jittered_linger_duration(
            self.duration,
            self.jitter_percent,
        )))
    }
}

fn jittered_linger_duration(duration: Duration, jitter_percent: u8) -> Duration {
    let base = duration.as_nanos();
    let spread = base.saturating_mul(jitter_percent as u128) / 100;
    let lower = base.saturating_sub(spread);
    duration_from_nanos(lower.saturating_add(random_u128(spread.saturating_mul(2))))
}

fn sleep(duration: Duration) -> Delay {
    Delay::new(duration)
}

fn random_duration_up_to(max_nanos: u128) -> Duration {
    duration_from_nanos(random_u128(max_nanos))
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
}

fn random_u128(max_inclusive: u128) -> u128 {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);
    if max_inclusive == 0 {
        return 0;
    }
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    let mut value = SEQUENCE.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed) ^ time;
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    (value as u128) % max_inclusive.saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MayStaticKey;

    #[derive(Clone)]
    struct FakeSink {
        statuses: Arc<Mutex<VecDeque<u16>>>,
        attempts: Arc<AtomicU64>,
        delay: Duration,
    }

    impl FakeSink {
        fn new(statuses: impl IntoIterator<Item = u16>) -> Self {
            Self {
                statuses: Arc::new(Mutex::new(statuses.into_iter().collect())),
                attempts: Arc::new(AtomicU64::new(0)),
                delay: Duration::ZERO,
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }
    }

    impl LogSink for FakeSink {
        fn send<'a>(
            &'a self,
            _metadata: &'a LogGroupMetadata,
            _logs: &'a [Log],
        ) -> Pin<Box<dyn Future<Output = Result<(), SlsClientError>> + Send + 'a>> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::Relaxed);
                sleep(self.delay).await;
                let status = self
                    .statuses
                    .lock()
                    .ok()
                    .and_then(|mut statuses| statuses.pop_front())
                    .unwrap_or(200);
                if (200..300).contains(&status) {
                    Ok(())
                } else {
                    Err(SlsClientError::Http {
                        status,
                        message: "test".into(),
                    })
                }
            })
        }
    }

    #[cfg(feature = "persist")]
    impl PersistentLogSink for FakeSink {
        fn send_idempotent<'a>(
            &'a self,
            metadata: &'a LogGroupMetadata,
            logs: &'a [Log],
            _spool_id: &'a [u8],
            _sequence_id: u64,
        ) -> Pin<Box<dyn Future<Output = Result<(), SlsClientError>> + Send + 'a>> {
            self.send(metadata, logs)
        }
    }

    fn client() -> SlsClient {
        SlsClient::builder()
            .access_key("key")
            .access_secret("secret")
            .expect("valid test secret")
            .endpoint("example.com")
            .project("project")
            .logstore("logstore")
            .enable_trace(false)
            .build()
            .expect("complete test client")
    }

    fn item(index: u32) -> Item {
        (
            Arc::new(LogGroupMetadata::new().with_topic("topic")),
            Log::new(index, None).with(MayStaticKey::from_static("message"), "value"),
        )
    }

    fn pending_batch() -> PendingBatch {
        let (metadata, log) = item(1);
        PendingBatch {
            encoded_len: calc_log_group_metadata_encoded_len(&metadata)
                + calc_log_group_log_encoded_len(&log),
            metadata,
            logs: vec![log],
        }
    }

    fn fake_worker(
        sink: FakeSink,
        config: ReporterConfig,
        state: Arc<State>,
    ) -> BatchWorker<FakeSink> {
        let (_sender, receiver) = async_channel::bounded(8);
        BatchWorker {
            consumer: receiver,
            client: sink,
            config,
            stats: state,
            vec_pool: Vec::new(),
            current: HashMap::new(),
            ready: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            in_flight_logs: 0,
            log_vec_capacity: 2,
            log_group_capacity: 2,
            vec_pool_capacity: 2,
        }
    }

    #[test]
    fn queue_reports_accepted_full_and_closed() {
        let reporter = Reporter::builder(client()).with_queue_capacity(1).build();
        let (metadata, log) = item(1);
        assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        let (metadata, log) = item(2);
        assert_eq!(reporter.try_report(metadata, log), ReportResult::Full);
        reporter.state.begin_close();
        reporter.producer.close();
        let (metadata, log) = item(3);
        assert_eq!(reporter.try_report(metadata, log), ReportResult::Closed);

        assert_eq!(
            reporter.stats(),
            ReporterStatsSnapshot {
                received: 3,
                accepted: 1,
                queue_full: 1,
                queue_closed: 1,
                dropped: 2,
                queue_depth: 1,
                ..ReporterStatsSnapshot::default()
            }
        );
    }

    #[test]
    fn batching_honors_count_and_encoded_size() {
        let config = ReporterConfig::default()
            .with_batch_max_count(2)
            .with_batch_max_bytes(usize::MAX);
        let (_sender, receiver) = async_channel::bounded(1);
        let state = Arc::new(State::default());
        let mut worker = BatchWorker {
            consumer: receiver,
            client: client(),
            config,
            stats: state,
            vec_pool: Vec::new(),
            current: HashMap::new(),
            ready: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            in_flight_logs: 0,
            log_vec_capacity: 2,
            log_group_capacity: 2,
            vec_pool_capacity: 2,
        };
        worker.push(item(1));
        assert_eq!(worker.current.len(), 1);
        worker.push(item(2));
        assert_eq!(worker.current.len(), 0);
        assert_eq!(worker.ready.front().map(|batch| batch.logs.len()), Some(2));

        worker.config.batch_max_count = usize::MAX;
        let third = item(3);
        worker.config.batch_max_bytes = calc_log_group_metadata_encoded_len(&third.0)
            + calc_log_group_log_encoded_len(&third.1);
        worker.push(third);
        assert_eq!(worker.ready.back().map(|batch| batch.logs.len()), Some(1));
    }

    #[test]
    fn oversized_log_is_dropped_and_accounted() {
        let config = ReporterConfig::default().with_batch_max_bytes(1);
        let (_sender, receiver) = async_channel::bounded(1);
        let state = Arc::new(State::default());
        let mut worker = BatchWorker {
            consumer: receiver,
            client: client(),
            config,
            stats: state.clone(),
            vec_pool: Vec::new(),
            current: HashMap::new(),
            ready: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            in_flight_logs: 0,
            log_vec_capacity: 2,
            log_group_capacity: 2,
            vec_pool_capacity: 2,
        };

        worker.push(item(1));

        assert!(worker.current.is_empty());
        assert!(worker.ready.is_empty());
        assert_eq!(state.snapshot().oversized, 1);
        assert_eq!(state.snapshot().send_failed, 1);
        assert_eq!(state.snapshot().batches_failed, 1);
        assert_eq!(state.snapshot().dropped, 1);
    }

    #[test]
    fn completed_batches_update_log_and_batch_counters() {
        let (_sender, receiver) = async_channel::bounded(1);
        let state = Arc::new(State::default());
        let mut worker = BatchWorker {
            consumer: receiver,
            client: client(),
            config: ReporterConfig::default(),
            stats: state.clone(),
            vec_pool: Vec::new(),
            current: HashMap::new(),
            ready: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            in_flight_logs: 3,
            log_vec_capacity: 2,
            log_group_capacity: 2,
            vec_pool_capacity: 2,
        };

        worker.finish_send(SendOutcome {
            log_count: 2,
            success: true,
        });
        worker.finish_send(SendOutcome {
            log_count: 1,
            success: false,
        });

        let stats = state.snapshot();
        assert_eq!(stats.sent, 2);
        assert_eq!(stats.batches_sent, 1);
        assert_eq!(stats.send_failed, 1);
        assert_eq!(stats.batches_failed, 1);
        assert_eq!(stats.dropped, 1);
    }

    #[test]
    fn retry_backoff_is_bounded_and_attempts_are_sanitized() {
        let config = ReporterConfig::default()
            .with_retry_max_attempts(0)
            .with_retry_base_delay(Duration::from_secs(20))
            .with_retry_max_delay(Duration::from_secs(10));
        assert_eq!(config.retry_max_attempts, 1);
        for attempt in 1..10 {
            assert!(retry_delay(&config, attempt) <= Duration::from_secs(10));
        }
    }

    #[test]
    fn linger_jitter_stays_within_configured_range() {
        for _ in 0..100 {
            let duration = jittered_linger_duration(Duration::from_secs(2), 20);
            assert!(duration >= Duration::from_millis(1_600));
            assert!(duration <= Duration::from_millis(2_400));
        }
    }

    #[test]
    fn upload_concurrency_never_exceeds_limit() {
        let state = Arc::new(State::default());
        let mut worker = fake_worker(
            FakeSink::new([200; 5]).with_delay(Duration::from_secs(1)),
            ReporterConfig::default().with_max_in_flight(2),
            state,
        );
        for _ in 0..5 {
            worker.ready.push_back(pending_batch());
        }

        worker.dispatch_ready();

        assert_eq!(worker.in_flight.len(), 2);
        assert_eq!(worker.ready.len(), 3);
    }

    #[tokio::test]
    async fn retries_twice_then_delivers_once() {
        let sink = FakeSink::new([503, 503, 200]);
        let attempts = sink.attempts.clone();
        let state = Arc::new(State::default());
        let config = ReporterConfig::default()
            .with_retry_max_attempts(5)
            .with_retry_base_delay(Duration::ZERO)
            .with_retry_max_delay(Duration::ZERO);

        let outcome = send_batch(sink, pending_batch(), config, state.clone()).await;

        assert!(outcome.success);
        assert_eq!(outcome.log_count, 1);
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert_eq!(state.snapshot().retried, 2);
    }

    #[tokio::test]
    async fn retryable_failure_stops_at_max_attempts() {
        let sink = FakeSink::new([503, 503, 503, 200]);
        let attempts = sink.attempts.clone();
        let state = Arc::new(State::default());
        let config = ReporterConfig::default()
            .with_retry_max_attempts(3)
            .with_retry_base_delay(Duration::ZERO)
            .with_retry_max_delay(Duration::ZERO);

        let outcome = send_batch(sink, pending_batch(), config, state.clone()).await;

        assert!(!outcome.success);
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert_eq!(state.snapshot().retried, 2);
    }

    #[tokio::test]
    async fn permanent_failure_is_not_retried() {
        let sink = FakeSink::new([403, 200]);
        let attempts = sink.attempts.clone();
        let state = Arc::new(State::default());
        let config = ReporterConfig::default()
            .with_retry_base_delay(Duration::ZERO)
            .with_retry_max_delay(Duration::ZERO);

        let outcome = send_batch(sink, pending_batch(), config, state.clone()).await;

        assert!(!outcome.success);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert_eq!(state.snapshot().retried, 0);
    }

    #[tokio::test]
    async fn graceful_shutdown_flushes_queued_log() {
        let state = Arc::new(State::default());
        let (sender, receiver) = async_channel::bounded(1);
        sender.try_send(item(1)).expect("queue has capacity");
        state.stats.queue_depth.store(1, Ordering::Relaxed);
        let sink = FakeSink::new([200]);
        let attempts = sink.attempts.clone();
        let mut worker = BatchWorker {
            consumer: receiver,
            client: sink,
            config: ReporterConfig::default(),
            stats: state.clone(),
            vec_pool: Vec::new(),
            current: HashMap::new(),
            ready: VecDeque::new(),
            in_flight: FuturesUnordered::new(),
            in_flight_logs: 0,
            log_vec_capacity: 2,
            log_group_capacity: 2,
            vec_pool_capacity: 2,
        };

        worker.graceful_shutdown().await;

        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert_eq!(state.snapshot().sent, 1);
        assert_eq!(state.snapshot().batches_sent, 1);
        assert_eq!(state.snapshot().queue_depth, 0);
    }

    #[tokio::test]
    async fn shutdown_deadline_abandons_hung_upload() {
        let state = Arc::new(State::default());
        let mut worker = fake_worker(
            FakeSink::new([200]).with_delay(Duration::from_secs(1)),
            ReporterConfig::default().with_shutdown_timeout(Duration::from_millis(5)),
            state.clone(),
        );
        worker.ready.push_back(pending_batch());

        worker.graceful_shutdown().await;

        assert_eq!(state.snapshot().dropped, 1);
        assert_eq!(state.snapshot().send_failed, 1);
        assert_eq!(state.snapshot().batches_failed, 1);
    }

    #[tokio::test]
    async fn shutdown_closes_admission_and_drains_empty_worker() {
        let reporter = Reporter::builder(client())
            .with_shutdown_timeout(Duration::from_millis(50))
            .build();
        let reporting = reporter
            .reporting_default()
            .await
            .expect("first reporting handle");
        reporting.with_graceful_shutdown(async {}).start().await;
        let (metadata, log) = item(1);
        assert_eq!(reporter.try_report(metadata, log), ReportResult::Closed);
        assert_eq!(reporter.stats().queue_closed, 1);
    }

    #[cfg(feature = "persist")]
    fn persistence_test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir()
            .join(format!(
                "aliyun-sls-reporter-{name}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock after epoch")
                    .as_nanos()
            ))
            .join("spool.sqlite3")
    }

    /// Wait until the writer handled every log admitted so far.
    #[cfg(feature = "persist")]
    async fn settle(reporter: &Reporter) {
        reporter
            .persistence
            .as_ref()
            .expect("persistent reporter")
            .flush()
            .await
            .expect("writer is running");
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn persistent_acceptance_is_committed_by_the_writer() {
        let path = persistence_test_path("commit");
        let reporter = Reporter::builder(client())
            .with_queue_capacity(2)
            .build_with_persistence(PersistenceConfig::new(1024 * 1024).path(path.clone()))
            .expect("build persistent reporter");
        let (metadata, log) = item(1);

        assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        settle(&reporter).await;

        let connection = rusqlite::Connection::open(&path).expect("open committed spool");
        let rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM pending_events", [], |row| row.get(0))
            .expect("read committed row");
        assert_eq!(rows, 1);
        assert_eq!(reporter.stats().persistence_committed, 1);
        assert_eq!(reporter.stats().persistence_pending_rows, 1);
        drop(connection);
        drop(reporter);
        let recovered = Reporter::builder(client())
            .build_with_persistence(PersistenceConfig::new(1024 * 1024).path(path.clone()))
            .expect("reopen persistent reporter");
        assert_eq!(recovered.stats().persistence_recovered_rows, 1);
        assert_eq!(recovered.stats().persistence_pending_rows, 1);
        assert_eq!(recovered.stats().persistence_committed, 0);
        drop(recovered);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn persistent_storage_eviction_updates_drop_counters() {
        let path = persistence_test_path("storage-eviction");
        let reporter = Reporter::builder(client())
            .build_with_persistence(PersistenceConfig::new(128 * 1024).path(path.clone()))
            .expect("build persistent reporter");
        for index in 1..=2 {
            let metadata = Arc::new(LogGroupMetadata::new());
            let log = Log::new(index, None)
                .with(MayStaticKey::from_static("message"), "x".repeat(70 * 1024));
            assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        }
        settle(&reporter).await;

        let stats = reporter.stats();
        assert_eq!(stats.persistence_committed, 2);
        assert_eq!(stats.persistence_evicted_storage, 1);
        assert_eq!(stats.persistence_pending_rows, 1);
        assert_eq!(stats.dropped, 1);
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// A row the writer cannot store (here: larger than the storage budget)
    /// is not reported to the caller, who already got `Accepted`; it is
    /// counted once in the write-failure, failure, and drop counters.
    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn persistent_write_failure_is_counted_not_returned() {
        let path = persistence_test_path("error");
        let reporter = Reporter::builder(client())
            .build_with_persistence(PersistenceConfig::new(128 * 1024).path(path.clone()))
            .expect("build persistent reporter");
        let metadata = Arc::new(LogGroupMetadata::new());
        let log =
            Log::new(1, None).with(MayStaticKey::from_static("message"), "x".repeat(100 * 1024));

        assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        settle(&reporter).await;
        let stats = reporter.stats();
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.persistence_committed, 0);
        assert_eq!(stats.persistence_write_failed, 1);
        assert_eq!(stats.persistence_failed, 1);
        assert_eq!(stats.dropped, 1);
        assert_eq!(stats.persistence_pending_rows, 0);
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[test]
    fn persistent_oversized_event_is_rejected_before_sqlite_admission() {
        let path = persistence_test_path("oversized-admission");
        let reporter = Reporter::builder(client())
            .with_batch_max_bytes(1)
            .build_with_persistence(PersistenceConfig::new(1024 * 1024).path(path.clone()))
            .expect("build persistent reporter");
        let (metadata, log) = item(1);

        assert_eq!(
            reporter.try_report(metadata, log),
            ReportResult::PersistenceFailed
        );

        let connection = rusqlite::Connection::open(&path).expect("open spool");
        let rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM pending_events", [], |row| row.get(0))
            .expect("count rows");
        assert_eq!(rows, 0);
        let stats = reporter.stats();
        assert_eq!(stats.oversized, 1);
        assert_eq!(stats.persistence_failed, 1);
        assert_eq!(stats.dropped, 1);
        assert_eq!(stats.accepted, 0);
        drop(connection);
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn persistent_shutdown_retains_durable_rows_without_dropping() {
        let path = persistence_test_path("shutdown-retains");
        let reporter = Reporter::builder(client())
            .with_shutdown_timeout(Duration::ZERO)
            .build_with_persistence(PersistenceConfig::new(1024 * 1024).path(path.clone()))
            .expect("build persistent reporter");
        let (metadata, log) = item(1);
        assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        settle(&reporter).await;
        let reporting = reporter
            .reporting_default()
            .await
            .expect("reporting handle");

        reporting.with_graceful_shutdown(async {}).start().await;

        let connection = rusqlite::Connection::open(&path).expect("open spool");
        let pending: i64 = connection
            .query_row("SELECT COUNT(*) FROM pending_events", [], |row| row.get(0))
            .expect("count pending");
        assert_eq!(pending, 1);
        assert_eq!(reporter.stats().dropped, 0);
        assert_eq!(reporter.stats().persistence_pending_rows, 1);
        drop(connection);
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn persistent_failed_cycle_counts_failure_without_drop() {
        let persistence = PersistenceSender::disconnected_for_test();
        let state = State::default();
        let (metadata, log) = item(1);
        let batch = PersistentBatch {
            sequence_id: 1,
            spool_id: vec![1].into_boxed_slice(),
            metadata: (*metadata).clone(),
            logs: vec![log],
            recovered_count: 0,
        };

        finish_persistent_cycle(&persistence, &batch, false, &state).await;

        let stats = state.snapshot();
        assert_eq!(stats.batches_failed, 1);
        assert_eq!(stats.send_failed, 1);
        assert_eq!(stats.persistence_failed_cycles, 1);
        assert_eq!(stats.dropped, 0);
    }

    #[cfg(feature = "persist")]
    async fn assigned_persistent_batch(
        name: &str,
    ) -> (std::path::PathBuf, Arc<PersistenceSender>, PersistentBatch) {
        let path = persistence_test_path(name);
        let state = Arc::new(State::default());
        let persistence = PersistenceSender::start(
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
            2,
            Some(b"fake-persistent-sink".to_vec().into_boxed_slice()),
            state.clone(),
        )
        .expect("start persistence");
        let (metadata, log) = item(1);
        assert!(
            persistence.enqueue(metadata, log, true, &state).is_ok(),
            "queue event"
        );
        let batch = persistence
            .next_batch(1, usize::MAX, true)
            .await
            .expect("assign batch")
            .expect("assigned batch");
        (path, persistence, batch)
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn persistent_fake_503_sink_retains_assigned_batch() {
        let (path, persistence, batch) = assigned_persistent_batch("fake-503").await;
        let state = State::default();
        let sink = FakeSink::new([503, 503]);
        let config = ReporterConfig::default()
            .with_retry_max_attempts(2)
            .with_retry_base_delay(Duration::ZERO)
            .with_retry_max_delay(Duration::ZERO);

        assert!(!persistent_send_cycle(&sink, &batch, &config, &state).await);
        let retained = persistence
            .next_batch(1, usize::MAX, true)
            .await
            .expect("reload retained batch")
            .expect("batch remains after 503");
        assert_eq!(retained.sequence_id, batch.sequence_id);
        assert_eq!(retained.logs, batch.logs);
        drop(persistence);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn upload_flag_is_stored_per_row_and_local_only_needs_a_spool() {
        let path = persistence_test_path("upload-flag");
        let reporter = Reporter::builder(client())
            .build_with_persistence(PersistenceConfig::new(1024 * 1024).path(path.clone()))
            .expect("build persistent reporter");
        for (index, upload) in [(1, None), (2, Some(false)), (3, Some(true))] {
            let (metadata, log) = item(index);
            let result = match upload {
                None => reporter.try_report(metadata, log),
                Some(upload) => reporter.try_report_with_upload(metadata, log, upload),
            };
            assert_eq!(result, ReportResult::Accepted);
        }
        settle(&reporter).await;
        let connection = rusqlite::Connection::open(&path).expect("open spool");
        let flags = connection
            .prepare("SELECT upload FROM pending_events ORDER BY id")
            .expect("prepare")
            .query_map([], |row| row.get::<_, bool>(0))
            .expect("query")
            .collect::<Result<Vec<_>, _>>()
            .expect("flags");
        assert_eq!(flags, vec![true, false, true]);
        drop(connection);
        drop(reporter);

        let memory = Reporter::builder(client()).build();
        let (metadata, log) = item(4);
        assert_eq!(
            memory.try_report_with_upload(metadata, log, false),
            ReportResult::NotStored
        );
        let stats = memory.stats();
        assert_eq!(
            (stats.received, stats.accepted, stats.queue_depth),
            (1, 0, 0)
        );
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// One idempotent request seen by [`RecordingSink`]:
    /// `(spool_id, sequence_id, log count)`.
    #[cfg(feature = "persist")]
    type RecordedCall = (Vec<u8>, u64, usize);

    /// Fake SLS endpoint that accepts every idempotent request and records it.
    #[cfg(feature = "persist")]
    #[derive(Clone, Default)]
    struct RecordingSink {
        calls: Arc<Mutex<Vec<RecordedCall>>>,
    }

    #[cfg(feature = "persist")]
    impl PersistentLogSink for RecordingSink {
        fn send_idempotent<'a>(
            &'a self,
            _metadata: &'a LogGroupMetadata,
            logs: &'a [Log],
            spool_id: &'a [u8],
            sequence_id: u64,
        ) -> Pin<Box<dyn Future<Output = Result<(), SlsClientError>> + Send + 'a>> {
            self.calls.lock().expect("record call").push((
                spool_id.to_vec(),
                sequence_id,
                logs.len(),
            ));
            Box::pin(async { Ok(()) })
        }
    }

    #[cfg(feature = "persist")]
    const CRASH_CHILD_ENV: &str = "ALIYUN_SLS_REPORTER_CRASH_CHILD";

    #[cfg(feature = "persist")]
    fn start_spool(path: &std::path::Path, state: Arc<State>) -> Arc<PersistenceSender> {
        PersistenceSender::start(
            PersistenceConfig::new(1024 * 1024).path(path.to_path_buf()),
            8,
            Some(b"crash-destination".to_vec().into_boxed_slice()),
            state,
        )
        .expect("start persistence")
    }

    /// Child half of the `kill -9` tests. `queued` commits one upload row and
    /// one local-only row; `remote-ack` additionally assigns the upload row
    /// and has the fake SLS accept it, then stops before local acknowledgement.
    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn crash_child_entry() {
        let Ok(spec) = std::env::var(CRASH_CHILD_ENV) else {
            return;
        };
        let (mode, path) = spec.split_once(':').expect("mode:path");
        let state = Arc::new(State::default());
        let persistence = start_spool(std::path::Path::new(path), state.clone());
        let (metadata, log) = item(1);
        assert!(persistence.enqueue(metadata, log, true, &state).is_ok());
        let (metadata, log) = item(2);
        assert!(persistence.enqueue(metadata, log, false, &state).is_ok());
        // Writes are asynchronous: report readiness only once both rows are
        // on disk, since the parent kills this process right after.
        persistence.flush().await.expect("writer is running");
        assert_eq!(state.snapshot().persistence_committed, 2);
        if mode == "remote-ack" {
            let batch = persistence
                .next_batch(10, usize::MAX, true)
                .await
                .expect("assign")
                .expect("batch");
            let sink = RecordingSink::default();
            assert!(persistent_send_cycle(&sink, &batch, &ReporterConfig::default(), &state).await);
            let spool: String = hex::encode(&batch.spool_id);
            println!("CHILD-READY seq={} spool={spool}", batch.sequence_id);
        } else {
            println!("CHILD-READY");
        }
        std::io::Write::flush(&mut std::io::stdout()).expect("flush");
        std::thread::park();
    }

    /// Run the child in `mode`, SIGKILL it once it reports readiness, and
    /// return the readiness line.
    #[cfg(feature = "persist")]
    fn run_and_kill_child(mode: &str, path: &std::path::Path) -> String {
        use std::io::BufRead;
        let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "reporter::tests::crash_child_entry",
                "--nocapture",
            ])
            .env(CRASH_CHILD_ENV, format!("{mode}:{}", path.display()))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("spawn child");
        let stdout = child.stdout.take().expect("child stdout");
        let ready = std::io::BufReader::new(stdout)
            .lines()
            .map(|line| line.expect("read child"))
            .find(|line| line.contains("CHILD-READY"))
            .expect("child readiness");
        child.kill().expect("SIGKILL child");
        let _ = child.wait().expect("reap child");
        ready
    }

    /// Reopen the spool, upload what is due through a recording fake SLS, and
    /// acknowledge it. Returns the recorded calls.
    #[cfg(feature = "persist")]
    async fn reopen_and_upload(path: &std::path::Path) -> Vec<RecordedCall> {
        let state = Arc::new(State::default());
        let persistence = start_spool(path, state.clone());
        assert_eq!(state.snapshot().persistence_recovered_rows, 1);
        assert_eq!(state.snapshot().persistence_pending_rows, 2);
        let batch = persistence
            .next_batch(10, usize::MAX, false)
            .await
            .expect("next batch")
            .expect("recovered row bypasses linger");
        let sink = RecordingSink::default();
        let config = ReporterConfig::default();
        assert!(persistent_send_cycle(&sink, &batch, &config, &state).await);
        finish_persistent_cycle(&persistence, &batch, true, &state).await;
        assert_eq!(state.snapshot().sent, 1);
        drop(persistence);

        let after = Arc::new(State::default());
        let persistence = start_spool(path, after.clone());
        assert_eq!(after.snapshot().persistence_recovered_rows, 0);
        assert!(
            persistence
                .next_batch(10, usize::MAX, true)
                .await
                .expect("next batch")
                .is_none(),
            "acknowledged row is not resent and local-only row is never sent"
        );
        assert_eq!(after.snapshot().persistence_pending_rows, 2);
        let calls = sink.calls.lock().expect("calls").clone();
        calls
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn killed_after_commit_before_send_uploads_after_restart() {
        let path = persistence_test_path("kill-before-send");
        run_and_kill_child("queued", &path);

        let calls = reopen_and_upload(&path).await;

        assert_eq!(calls.len(), 1);
        assert_eq!(
            (calls[0].1, calls[0].2),
            (1, 1),
            "first seqid, upload row only"
        );
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn killed_after_remote_ack_before_local_ack_resends_same_seqid() {
        let path = persistence_test_path("kill-after-remote-ack");
        let ready = run_and_kill_child("remote-ack", &path);

        let calls = reopen_and_upload(&path).await;

        assert_eq!(calls.len(), 1);
        let spool: String = hex::encode(&calls[0].0);
        assert_eq!(
            ready,
            format!("CHILD-READY seq={} spool={spool}", calls[0].1),
            "restart resends with the seqid and spool key SLS already received"
        );
        assert_eq!(calls[0].2, 1);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// Queue one row, then round-trip a `NextBatch` command through the
    /// writer. The writer handles commands in order, so when this returns it
    /// has finished the commit group, including its wake decision.
    #[cfg(feature = "persist")]
    async fn commit_and_settle(
        persistence: &PersistenceSender,
        state: &State,
        index: u32,
        upload: bool,
    ) {
        let (metadata, log) = item(index);
        assert!(persistence.enqueue(metadata, log, upload, state).is_ok());
        let _ = persistence
            .next_batch(usize::MAX, usize::MAX, false)
            .await
            .expect("writer is running");
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn local_only_commit_does_not_wake_uploader() {
        let path = persistence_test_path("local-only-no-wake");
        let state = Arc::new(State::default());
        let persistence = start_spool(&path, state.clone());

        commit_and_settle(&persistence, &state, 1, false).await;
        commit_and_settle(&persistence, &state, 2, false).await;

        assert!(
            !persistence.take_wake(),
            "local-only rows can never form a batch, so they must not wake the uploader"
        );
        drop(persistence);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn waiting_uploader_is_woken_by_upload_row_only() {
        let path = persistence_test_path("upload-wakes");
        let state = Arc::new(State::default());
        let persistence = start_spool(&path, state.clone());
        let (_stop, shutdown) = async_channel::bounded::<()>(1);
        let config = ReporterConfig::default()
            .with_linger(Duration::from_millis(10))
            .with_linger_jitter_percent(0);
        // The spool is empty, so the uploader's first look finds nothing and
        // it waits for a wake-up; without one it would wait forever, which
        // the timeout below turns into a failure.
        let waiting = tokio::spawn({
            let persistence = persistence.clone();
            async move {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    next_persistent_batch(&persistence, &config, &shutdown),
                )
                .await
            }
        });

        commit_and_settle(&persistence, &state, 1, false).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        commit_and_settle(&persistence, &state, 2, true).await;

        let batch = waiting
            .await
            .expect("uploader task")
            .expect("upload row wakes the waiting uploader")
            .expect("next batch")
            .expect("a local-only wake-up would have returned no batch");
        assert_eq!(batch.logs, vec![item(2).1], "only the upload row is sent");
        drop(persistence);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn unsent_rows_count_upload_backlog_until_acknowledged() {
        let path = persistence_test_path("unsent-rows");
        let state = Arc::new(State::default());
        let persistence = start_spool(&path, state.clone());
        for (index, upload) in [(1, true), (2, false), (3, true)] {
            let (metadata, log) = item(index);
            assert!(persistence.enqueue(metadata, log, upload, &state).is_ok());
        }
        persistence.flush().await.expect("writer is running");
        assert_eq!(state.snapshot().persistence_unsent_rows, 2);

        let batch = persistence
            .next_batch(1, usize::MAX, true)
            .await
            .expect("next batch")
            .expect("one upload row");
        assert_eq!(
            state.snapshot().persistence_unsent_rows,
            2,
            "an assigned batch is still unsent"
        );
        let sink = RecordingSink::default();
        assert!(persistent_send_cycle(&sink, &batch, &ReporterConfig::default(), &state).await);
        finish_persistent_cycle(&persistence, &batch, true, &state).await;

        // 3 written, 2 marked for upload, 1 sent: 2 - 1 = 1.
        assert_eq!(state.snapshot().persistence_unsent_rows, 1);
        assert_eq!(state.snapshot().persistence_pending_rows, 3);
        let connection = rusqlite::Connection::open(&path).expect("open spool");
        let unsent: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pending_events WHERE upload = 1 AND sent = 0",
                [],
                |row| row.get(0),
            )
            .expect("count unsent rows");
        assert_eq!(unsent, 1, "the gauge matches the spool");
        drop(connection);
        drop(persistence);

        let reopened = Arc::new(State::default());
        let persistence = start_spool(&path, reopened.clone());
        assert_eq!(reopened.snapshot().persistence_unsent_rows, 1);
        drop(persistence);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// Hold the SQLite write lock from another connection, the way a
    /// `sqlite3` shell running `BEGIN EXCLUSIVE` on the spool would.
    #[cfg(feature = "persist")]
    fn lock_spool(path: &std::path::Path) -> rusqlite::Connection {
        let connection = rusqlite::Connection::open(path).expect("open locking connection");
        connection
            .busy_timeout(Duration::from_secs(5))
            .expect("locker busy timeout");
        connection
            .execute_batch("BEGIN EXCLUSIVE")
            .expect("take exclusive lock");
        connection
    }

    #[cfg(feature = "persist")]
    fn count_rows(path: &std::path::Path) -> i64 {
        rusqlite::Connection::open(path)
            .expect("open spool")
            .query_row("SELECT COUNT(*) FROM pending_events", [], |row| row.get(0))
            .expect("count rows")
    }

    /// The caller of `try_report` never waits for SQLite: with the spool
    /// locked by another connection, 1000 admissions each return at once,
    /// and the rows are written once the lock is released.
    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn locked_spool_does_not_block_admission_and_rows_follow_unlock() {
        const CALLS: u32 = 1000;
        let path = persistence_test_path("locked-admission");
        // Default busy timeout (5 s): the writer is still waiting for the lock
        // when it is released below, so no row should fail.
        let reporter = Reporter::builder(client())
            .build_with_persistence(PersistenceConfig::new(16 * 1024 * 1024).path(path.clone()))
            .expect("build persistent reporter");
        let lock = lock_spool(&path);

        let mut samples = Vec::with_capacity(CALLS as usize);
        for index in 0..CALLS {
            let (metadata, log) = item(index);
            let started = std::time::Instant::now();
            let result = reporter.try_report(metadata, log);
            let elapsed = started.elapsed();
            // Generous against scheduler noise; the old synchronous admission
            // waited for the 5 s busy timeout here. Checked per call so a
            // regression fails on the first call instead of after 1000 waits.
            assert!(
                elapsed < Duration::from_millis(50),
                "admission {index} took {elapsed:?}"
            );
            assert_eq!(result, ReportResult::Accepted);
            samples.push(elapsed);
        }
        assert_eq!(
            reporter.stats().persistence_committed,
            0,
            "nothing can be written while the lock is held"
        );
        lock.execute_batch("COMMIT").expect("release lock");
        drop(lock);
        settle(&reporter).await;

        samples.sort();
        let at = |quantile: f64| samples[((samples.len() - 1) as f64 * quantile) as usize];
        println!(
            "locked try_report n={} min={:?} p50={:?} p90={:?} p99={:?} p999={:?} max={:?}",
            samples.len(),
            samples[0],
            at(0.5),
            at(0.9),
            at(0.99),
            at(0.999),
            samples[samples.len() - 1]
        );
        let stats = reporter.stats();
        assert_eq!(
            (stats.accepted, stats.persistence_committed),
            (u64::from(CALLS), u64::from(CALLS))
        );
        assert_eq!(stats.persistence_write_failed, 0);
        assert_eq!(count_rows(&path), i64::from(CALLS));
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// While the writer is stuck, the bounded queue in front of it fills and
    /// further logs are rejected immediately and counted, never waited on.
    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn full_writer_queue_drops_and_counts() {
        const CALLS: u32 = 20;
        let path = persistence_test_path("queue-full");
        let reporter = Reporter::builder(client())
            .with_queue_capacity(4)
            .build_with_persistence(PersistenceConfig::new(1024 * 1024).path(path.clone()))
            .expect("build persistent reporter");
        let lock = lock_spool(&path);

        let accepted = (0..CALLS)
            .filter(|&index| {
                let (metadata, log) = item(index);
                match reporter.try_report(metadata, log) {
                    ReportResult::Accepted => true,
                    ReportResult::PersistenceFailed => false,
                    other => panic!("unexpected admission result {other:?}"),
                }
            })
            .count() as u64;
        // The queue holds 4; the writer may already have taken one more.
        assert!((4..=5).contains(&accepted), "accepted {accepted}");
        let rejected = u64::from(CALLS) - accepted;
        let stats = reporter.stats();
        assert_eq!(stats.queue_full, rejected);
        assert_eq!(stats.persistence_failed, rejected);
        assert_eq!(stats.dropped, rejected);
        assert_eq!(stats.received, u64::from(CALLS));

        lock.execute_batch("COMMIT").expect("release lock");
        drop(lock);
        settle(&reporter).await;
        assert_eq!(reporter.stats().persistence_committed, accepted);
        assert_eq!(count_rows(&path), accepted as i64);
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// A lock held longer than `busy_timeout` makes the write fail. The
    /// callers already got `Accepted`; the rows are counted as write failures
    /// and the writer keeps serving later rows.
    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn write_failure_under_lock_is_counted_and_writer_recovers() {
        const CALLS: u32 = 10;
        let path = persistence_test_path("write-failed");
        let reporter = Reporter::builder(client())
            .build_with_persistence(
                PersistenceConfig::new(1024 * 1024)
                    .path(path.clone())
                    .busy_timeout(Duration::from_millis(50)),
            )
            .expect("build persistent reporter");
        let lock = lock_spool(&path);
        for index in 0..CALLS {
            let (metadata, log) = item(index);
            assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        }
        settle(&reporter).await;

        let stats = reporter.stats();
        assert_eq!(stats.persistence_committed, 0);
        assert_eq!(stats.persistence_write_failed, u64::from(CALLS));
        assert_eq!(stats.persistence_failed, u64::from(CALLS));
        assert_eq!(stats.dropped, u64::from(CALLS));
        assert_eq!(stats.queue_depth, 0);

        lock.execute_batch("COMMIT").expect("release lock");
        drop(lock);
        let (metadata, log) = item(CALLS);
        assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        settle(&reporter).await;
        assert_eq!(reporter.stats().persistence_committed, 1);
        assert_eq!(count_rows(&path), 1);
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// Without an SLS client every log is still stored with its upload flag
    /// and nothing is sent. A later start with a client uploads the rows that
    /// were marked for upload.
    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn local_only_reporter_stores_without_client_and_later_client_uploads() {
        let path = persistence_test_path("local-only");
        let reporter = Reporter::local_only(
            ReporterConfig::default(),
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
        )
        .expect("build local-only reporter");
        assert!(reporter.client.is_none(), "no HTTP client exists");
        for (index, upload) in [(1, None), (2, Some(false)), (3, Some(true))] {
            let (metadata, log) = item(index);
            let result = match upload {
                None => reporter.try_report(metadata, log),
                Some(upload) => reporter.try_report_with_upload(metadata, log, upload),
            };
            assert_eq!(result, ReportResult::Accepted);
        }
        let reporting = reporter
            .reporting_default()
            .await
            .expect("reporting handle");
        reporting.with_graceful_shutdown(async {}).start().await;

        let stats = reporter.stats();
        assert_eq!(stats.persistence_committed, 3);
        assert_eq!(
            (
                stats.sent,
                stats.batches_sent,
                stats.batches_failed,
                stats.retried
            ),
            (0, 0, 0, 0),
            "no send was attempted"
        );
        let flags = rusqlite::Connection::open(&path)
            .expect("open spool")
            .prepare("SELECT upload, sent, batch_seq IS NULL FROM pending_events ORDER BY id")
            .expect("prepare")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("query")
            .collect::<Result<Vec<(bool, bool, bool)>, _>>()
            .expect("flags");
        assert_eq!(
            flags,
            vec![
                (true, false, true),
                (false, false, true),
                (true, false, true)
            ],
            "upload flag kept, nothing assigned or sent"
        );
        drop(reporter);

        let state = Arc::new(State::default());
        let persistence = start_spool(&path, state.clone());
        assert_eq!(state.snapshot().persistence_recovered_rows, 2);
        let batch = persistence
            .next_batch(10, usize::MAX, false)
            .await
            .expect("next batch")
            .expect("rows stored without a client are due");
        assert_eq!(batch.logs, vec![item(1).1, item(3).1]);
        let sink = RecordingSink::default();
        assert!(persistent_send_cycle(&sink, &batch, &ReporterConfig::default(), &state).await);
        finish_persistent_cycle(&persistence, &batch, true, &state).await;
        assert_eq!(state.snapshot().sent, 2);
        assert_eq!(sink.calls.lock().expect("calls").len(), 1);
        drop(persistence);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// Graceful shutdown drains the writer queue before it returns, so logs
    /// admitted before shutdown are on disk even if the writer was behind.
    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn graceful_shutdown_drains_the_writer_queue() {
        const CALLS: u32 = 100;
        let path = persistence_test_path("shutdown-drains");
        let reporter = Reporter::local_only(
            ReporterConfig::default().with_shutdown_timeout(Duration::from_secs(10)),
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
        )
        .expect("build local-only reporter");
        let lock = lock_spool(&path);
        for index in 0..CALLS {
            let (metadata, log) = item(index);
            assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        }
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            lock.execute_batch("COMMIT").expect("release lock");
        });
        let reporting = reporter
            .reporting_default()
            .await
            .expect("reporting handle");

        reporting.with_graceful_shutdown(async {}).start().await;

        assert_eq!(reporter.stats().persistence_committed, u64::from(CALLS));
        release.join().expect("release thread");
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    /// The shutdown drain is bounded by the shutdown timeout even while the
    /// writer waits for a lock far longer than that.
    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn graceful_shutdown_drain_is_bounded_by_the_shutdown_timeout() {
        let path = persistence_test_path("shutdown-bounded");
        let reporter = Reporter::builder(client())
            .with_shutdown_timeout(Duration::from_millis(200))
            .build_with_persistence(
                PersistenceConfig::new(1024 * 1024)
                    .path(path.clone())
                    .busy_timeout(Duration::from_secs(30)),
            )
            .expect("build persistent reporter");
        let lock = lock_spool(&path);
        let (metadata, log) = item(1);
        assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
        let reporting = reporter
            .reporting_default()
            .await
            .expect("reporting handle");

        let started = std::time::Instant::now();
        reporting.with_graceful_shutdown(async {}).start().await;
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown took {elapsed:?}"
        );
        assert_eq!(reporter.stats().persistence_committed, 0);
        lock.execute_batch("COMMIT").expect("release lock");
        drop(lock);
        drop(reporter);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[cfg(feature = "persist")]
    #[tokio::test]
    async fn persistent_fake_timeout_retains_assigned_batch() {
        let (path, persistence, batch) = assigned_persistent_batch("fake-timeout").await;
        let state = State::default();
        let sink = FakeSink::new([200]).with_delay(Duration::from_secs(1));
        let config = ReporterConfig::default().with_retry_max_attempts(1);
        let send = persistent_send_cycle(&sink, &batch, &config, &state).fuse();
        let timeout = sleep(Duration::from_millis(5)).fuse();
        futures_util::pin_mut!(send, timeout);

        assert!(matches!(
            futures_util::future::select(send, timeout).await,
            futures_util::future::Either::Right(_)
        ));
        let retained = persistence
            .next_batch(1, usize::MAX, true)
            .await
            .expect("reload retained batch")
            .expect("batch remains after timeout");
        assert_eq!(retained.sequence_id, batch.sequence_id);
        assert_eq!(retained.logs, batch.logs);
        drop(persistence);
        let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
    }
}
