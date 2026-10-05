use crate::{
    Log, LogGroupMetadata,
    proto::{
        calc_log_group_log_encoded_len, calc_log_group_metadata_encoded_len,
        decode_persisted_event, encode_persisted_event,
    },
    reporter::State,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering, mpsc},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const SCHEMA_VERSION: i64 = 4;
const CACHE_SIZE_KIB: i64 = 2 * 1024;
const MAX_WAL_AUTOCHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_GROUP_COMMIT_EVENTS: usize = 32;
const MAX_GROUP_COMMIT_BYTES: usize = 1024 * 1024;
const EVICTION_CHUNK_ROWS: i64 = 256;

/// Configuration for the SQLite-backed event spool.
///
/// [`PersistenceConfig::new`] requires the conservative physical storage
/// target. The path defaults to
/// `<current-executable>.sls-spool.sqlite3` beside the executable. Optional
/// event-count and age limits are applied in addition to the byte target.
#[derive(Clone, Debug)]
pub struct PersistenceConfig {
    pub(crate) max_storage_bytes: u64,
    pub(crate) path: Option<PathBuf>,
    pub(crate) max_events: Option<u64>,
    pub(crate) max_age: Option<Duration>,
}

/// An error while configuring or operating the persistent event spool.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PersistenceError {
    /// A persistence limit was zero or otherwise unusable.
    #[error("invalid persistence configuration: {0}")]
    InvalidConfig(&'static str),
    /// The default spool path could not be derived from the current executable.
    #[error("cannot resolve the default persistence path")]
    DefaultPath,
    /// A filesystem operation failed.
    #[error("persistence filesystem error: {0}")]
    Io(#[from] std::io::Error),
    /// SQLite rejected an operation.
    #[error("persistence SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// An event could not be encoded for durable storage.
    #[error("persistence serialization error: {0}")]
    Serialization(#[source] std::io::Error),
    /// The database schema is newer than this library understands.
    #[error("unsupported persistence schema version {0}")]
    UnsupportedSchema(i64),
    /// The spool was created for a different endpoint, project, or logstore.
    #[error("persistence spool destination does not match this SLS client")]
    DestinationMismatch,
    /// Stored spool totals do not match the pending event table.
    #[error("persistence spool totals are inconsistent")]
    InconsistentState,
    /// The event cannot fit within the configured spool budget.
    #[error("event exceeds the persistence storage budget")]
    EventTooLarge,
    /// The dedicated persistence writer stopped.
    #[error("persistence writer stopped")]
    WriterStopped,
}

impl PersistenceConfig {
    /// Create a persistence configuration with a target whole-spool byte budget.
    ///
    /// Retention reserves conservative headroom for SQLite metadata and its WAL,
    /// but filesystem allocation and WAL checkpoint timing make this a target
    /// rather than an exact upper bound on allocated bytes.
    pub fn new(max_storage_bytes: u64) -> Self {
        Self {
            max_storage_bytes,
            path: None,
            max_events: None,
            max_age: None,
        }
    }

    /// Store the spool at an explicit filesystem path.
    ///
    /// On Unix the database file is created and maintained with mode `0600`.
    pub fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Limit the number of pending events retained in the spool.
    ///
    /// When admitting a new event, the oldest eligible unassigned rows are
    /// evicted first. An assigned upload batch is never evicted.
    pub fn max_events(mut self, max_events: u64) -> Self {
        self.max_events = Some(max_events);
        self
    }

    /// Limit how long pending events are retained in the spool.
    ///
    /// Expired unassigned rows are removed during admission and startup limit
    /// enforcement. An assigned upload batch remains until local
    /// acknowledgement.
    pub fn max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    pub(crate) fn validate(&self) -> Result<(), PersistenceError> {
        if self.max_storage_bytes == 0 {
            return Err(PersistenceError::InvalidConfig(
                "max_storage_bytes must be greater than zero",
            ));
        }
        if self.max_events == Some(0) {
            return Err(PersistenceError::InvalidConfig(
                "max_events must be greater than zero",
            ));
        }
        if self.max_age == Some(Duration::ZERO) {
            return Err(PersistenceError::InvalidConfig(
                "max_age must be greater than zero",
            ));
        }
        if self
            .path
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(PersistenceError::InvalidConfig("path must not be empty"));
        }
        Ok(())
    }

    fn resolve_path(&self) -> Result<PathBuf, PersistenceError> {
        if let Some(path) = &self.path {
            return Ok(path.clone());
        }
        let executable = std::env::current_exe().map_err(|_| PersistenceError::DefaultPath)?;
        let directory = executable.parent().ok_or(PersistenceError::DefaultPath)?;
        let name = executable
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or(PersistenceError::DefaultPath)?;
        Ok(directory.join(format!("{name}.sls-spool.sqlite3")))
    }

    fn payload_budget(&self) -> u64 {
        self.max_storage_bytes
            .saturating_sub(self.max_storage_bytes / 4)
    }
}

pub(crate) struct PersistenceSender {
    sender: async_channel::Sender<Command>,
    wake_receiver: async_channel::Receiver<()>,
}

pub(crate) enum AdmissionFailure {
    Full,
    Closed,
    Store,
}

type InsertAcknowledgement = mpsc::SyncSender<Result<(), PersistenceError>>;

enum Command {
    Insert {
        metadata: Arc<LogGroupMetadata>,
        log: Box<Log>,
        acknowledgement: InsertAcknowledgement,
    },
    NextBatch {
        max_count: usize,
        max_bytes: usize,
        force: bool,
        acknowledgement: async_channel::Sender<Result<Option<PersistentBatch>, PersistenceError>>,
    },
    Acknowledge {
        sequence_id: u64,
        acknowledgement: async_channel::Sender<Result<InsertOutcome, PersistenceError>>,
    },
}

struct PreparedInsert {
    payload: Vec<u8>,
    created_at_ms: i64,
    acknowledgement: InsertAcknowledgement,
}

pub(crate) struct PersistentBatch {
    pub(crate) sequence_id: u64,
    pub(crate) spool_id: Box<[u8]>,
    pub(crate) metadata: LogGroupMetadata,
    pub(crate) logs: Vec<Log>,
    pub(crate) recovered_count: usize,
}

struct Store {
    connection: Connection,
    config: PersistenceConfig,
    #[cfg(test)]
    committed_transactions: u64,
    #[cfg(test)]
    fail_group_commit: bool,
}

#[derive(Default)]
pub(crate) struct InsertOutcome {
    pub(crate) evicted_age: u64,
    pub(crate) evicted_count: u64,
    pub(crate) evicted_storage: u64,
    pub(crate) evicted_bytes: u64,
    pub(crate) pending_rows: u64,
    pub(crate) pending_bytes: u64,
}

impl PersistenceSender {
    #[cfg(test)]
    pub(crate) fn disconnected_for_test() -> Self {
        let (sender, receiver) = async_channel::bounded(1);
        drop(receiver);
        let (_wake_sender, wake_receiver) = async_channel::bounded(1);
        Self {
            sender,
            wake_receiver,
        }
    }

    pub(crate) fn start(
        config: PersistenceConfig,
        queue_capacity: usize,
        destination_fingerprint: Box<[u8]>,
        state: Arc<State>,
    ) -> Result<Arc<Self>, PersistenceError> {
        config.validate()?;
        let path = config.resolve_path()?;
        let (sender, receiver) = async_channel::bounded(queue_capacity.max(1));
        let (wake_sender, wake_receiver) = async_channel::bounded(1);
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("aliyun-sls-sqlite-writer".into())
            .spawn(move || {
                match Store::open_for_destination(config, &path, &destination_fingerprint) {
                    Ok(mut store) => {
                        let pending =
                            store
                                .enforce_existing_limits(now_millis())
                                .and_then(|outcome| {
                                    update_persistence_stats(&state, &outcome);
                                    store.recover_pending()
                                });
                        match pending {
                            Ok((rows, bytes)) => {
                                state
                                    .stats
                                    .persistence_recovered_rows
                                    .fetch_add(rows, Ordering::Relaxed);
                                state
                                    .stats
                                    .persistence_pending_rows
                                    .store(rows, Ordering::Relaxed);
                                state
                                    .stats
                                    .persistence_pending_bytes
                                    .store(bytes, Ordering::Relaxed);
                                if startup_sender.send(Ok(())).is_ok() {
                                    writer_loop(&mut store, receiver, wake_sender, &state);
                                }
                            }
                            Err(error) => {
                                let _ = startup_sender.send(Err(error));
                            }
                        }
                    }
                    Err(error) => {
                        let _ = startup_sender.send(Err(error));
                    }
                }
            })
            .map_err(PersistenceError::Io)?;
        startup_receiver
            .recv()
            .map_err(|_| PersistenceError::WriterStopped)??;
        Ok(Arc::new(Self {
            sender,
            wake_receiver,
        }))
    }

    pub(crate) fn commit(
        &self,
        metadata: Arc<LogGroupMetadata>,
        log: Log,
        state: &State,
    ) -> Result<(), AdmissionFailure> {
        let (acknowledgement, result) = mpsc::sync_channel(1);
        state.stats.queue_depth.fetch_add(1, Ordering::Relaxed);
        match self.sender.try_send(Command::Insert {
            metadata,
            log: Box::new(log),
            acknowledgement,
        }) {
            Ok(()) => result
                .recv()
                .map_err(|_| AdmissionFailure::Closed)?
                .map_err(|_| AdmissionFailure::Store),
            Err(async_channel::TrySendError::Full(_)) => {
                decrement_queue_depth(state);
                Err(AdmissionFailure::Full)
            }
            Err(async_channel::TrySendError::Closed(_)) => {
                decrement_queue_depth(state);
                Err(AdmissionFailure::Closed)
            }
        }
    }

    pub(crate) async fn next_batch(
        &self,
        max_count: usize,
        max_bytes: usize,
        force: bool,
    ) -> Result<Option<PersistentBatch>, PersistenceError> {
        let (acknowledgement, result) = async_channel::bounded(1);
        self.sender
            .send(Command::NextBatch {
                max_count,
                max_bytes,
                force,
                acknowledgement,
            })
            .await
            .map_err(|_| PersistenceError::WriterStopped)?;
        result
            .recv()
            .await
            .map_err(|_| PersistenceError::WriterStopped)?
    }

    pub(crate) async fn acknowledge(
        &self,
        sequence_id: u64,
    ) -> Result<InsertOutcome, PersistenceError> {
        let (acknowledgement, result) = async_channel::bounded(1);
        self.sender
            .send(Command::Acknowledge {
                sequence_id,
                acknowledgement,
            })
            .await
            .map_err(|_| PersistenceError::WriterStopped)?;
        result
            .recv()
            .await
            .map_err(|_| PersistenceError::WriterStopped)?
    }

    pub(crate) async fn notified(&self) {
        let _ = self.wake_receiver.recv().await;
    }
}

fn writer_loop(
    store: &mut Store,
    receiver: async_channel::Receiver<Command>,
    wake_sender: async_channel::Sender<()>,
    state: &State,
) {
    let mut pending = None;
    loop {
        let (command, decrement_first_insert) = match pending.take() {
            Some(command) => (command, false),
            None => match receiver.recv_blocking() {
                Ok(command) => (command, true),
                Err(_) => break,
            },
        };
        match command {
            Command::Insert {
                metadata,
                log,
                acknowledgement,
            } => {
                if decrement_first_insert {
                    decrement_queue_depth(state);
                }
                let mut commands = Vec::with_capacity(MAX_GROUP_COMMIT_EVENTS);
                let mut encoded_bytes = 0usize;
                let mut next = Some((metadata, log, acknowledgement));
                while let Some((metadata, log, acknowledgement)) = next.take() {
                    match encode_persisted_event(&metadata, &log) {
                        Ok(payload)
                            if payload.len() as u64 <= store.config.payload_budget()
                                && (commands.is_empty()
                                    || encoded_bytes.saturating_add(payload.len())
                                        <= MAX_GROUP_COMMIT_BYTES) =>
                        {
                            encoded_bytes = encoded_bytes.saturating_add(payload.len());
                            commands.push(PreparedInsert {
                                payload,
                                created_at_ms: now_millis(),
                                acknowledgement,
                            });
                        }
                        Ok(payload) if payload.len() as u64 > store.config.payload_budget() => {
                            let _ = acknowledgement.send(Err(PersistenceError::EventTooLarge));
                        }
                        Ok(_) => {
                            pending = Some(Command::Insert {
                                metadata,
                                log,
                                acknowledgement,
                            });
                            break;
                        }
                        Err(error) => {
                            let _ =
                                acknowledgement.send(Err(PersistenceError::Serialization(error)));
                        }
                    }
                    if commands.len() >= MAX_GROUP_COMMIT_EVENTS {
                        break;
                    }
                    match receiver.try_recv() {
                        Ok(Command::Insert {
                            metadata,
                            log,
                            acknowledgement,
                        }) => {
                            decrement_queue_depth(state);
                            next = Some((metadata, log, acknowledgement));
                        }
                        Ok(command) => {
                            pending = Some(command);
                            break;
                        }
                        Err(_) => break,
                    }
                }

                if commands.is_empty() {
                    continue;
                }
                match store.insert_group(&commands) {
                    Ok(results) => {
                        let mut inserted = false;
                        for (command, result) in commands.into_iter().zip(results) {
                            if let Ok(outcome) = &result {
                                inserted = true;
                                update_persistence_stats(state, outcome);
                            }
                            let _ = command.acknowledgement.send(result.map(|_| ()));
                        }
                        if inserted {
                            let _ = wake_sender.try_send(());
                        }
                    }
                    Err(error) => {
                        let message = error.to_string();
                        for command in commands {
                            let error = PersistenceError::Sqlite(
                                rusqlite::Error::InvalidParameterName(message.clone()),
                            );
                            let _ = command.acknowledgement.send(Err(error));
                        }
                    }
                }
            }
            Command::NextBatch {
                max_count,
                max_bytes,
                force,
                acknowledgement,
            } => {
                let _ =
                    acknowledgement.send_blocking(store.next_batch(max_count, max_bytes, force));
            }
            Command::Acknowledge {
                sequence_id,
                acknowledgement,
            } => {
                let result = store.acknowledge(sequence_id);
                if let Ok(outcome) = &result {
                    update_persistence_stats(state, outcome);
                }
                let _ = acknowledgement.send_blocking(result);
            }
        }
    }
}

fn update_persistence_stats(state: &State, outcome: &InsertOutcome) {
    state
        .stats
        .persistence_evicted_age
        .fetch_add(outcome.evicted_age, Ordering::Relaxed);
    state
        .stats
        .persistence_evicted_count
        .fetch_add(outcome.evicted_count, Ordering::Relaxed);
    state
        .stats
        .persistence_evicted_storage
        .fetch_add(outcome.evicted_storage, Ordering::Relaxed);
    state
        .stats
        .persistence_evicted_bytes
        .fetch_add(outcome.evicted_bytes, Ordering::Relaxed);
    state
        .stats
        .persistence_pending_rows
        .store(outcome.pending_rows, Ordering::Relaxed);
    state
        .stats
        .persistence_pending_bytes
        .store(outcome.pending_bytes, Ordering::Relaxed);
    let evicted_rows = outcome
        .evicted_age
        .saturating_add(outcome.evicted_count)
        .saturating_add(outcome.evicted_storage);
    state
        .stats
        .dropped
        .fetch_add(evicted_rows, Ordering::Relaxed);
}

fn decrement_queue_depth(state: &State) {
    let _ = state
        .stats
        .queue_depth
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
            Some(depth.saturating_sub(1))
        });
}

impl Store {
    #[cfg(test)]
    fn open(config: PersistenceConfig, path: &Path) -> Result<Self, PersistenceError> {
        Self::open_for_destination(config, path, b"test-destination")
    }

    fn open_for_destination(
        config: PersistenceConfig,
        path: &Path,
        destination_fingerprint: &[u8],
    ) -> Result<Self, PersistenceError> {
        config.validate()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        secure_spool_file(path)?;
        let mut connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "auto_vacuum", "NONE")?;
        connection.pragma_update(None, "mmap_size", 0)?;
        connection.pragma_update(None, "cache_size", -CACHE_SIZE_KIB)?;
        let page_size: i64 = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
        let checkpoint_bytes = config
            .max_storage_bytes
            .saturating_div(4)
            .min(MAX_WAL_AUTOCHECKPOINT_BYTES)
            .max(page_size.max(1) as u64);
        let checkpoint_pages = i64::try_from(
            checkpoint_bytes
                .saturating_div(page_size.max(1) as u64)
                .max(1),
        )
        .unwrap_or(i64::MAX);
        connection.pragma_update(None, "wal_autocheckpoint", checkpoint_pages)?;
        connection.pragma_update(
            None,
            "journal_size_limit",
            i64::try_from(checkpoint_bytes).unwrap_or(i64::MAX),
        )?;

        let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version > SCHEMA_VERSION {
            return Err(PersistenceError::UnsupportedSchema(version));
        }
        if version < SCHEMA_VERSION {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            match version {
                0 => {
                    transaction.execute_batch(
                        "CREATE TABLE pending_events (
                    id INTEGER PRIMARY KEY,
                    created_at_ms INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    payload_bytes INTEGER NOT NULL,
                    batch_seq INTEGER,
                    recovered INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS pending_events_created
                    ON pending_events(created_at_ms, id);
                CREATE INDEX IF NOT EXISTS pending_events_batch
                    ON pending_events(batch_seq, id);
                CREATE TABLE spool_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    spool_id BLOB NOT NULL,
                    next_seq INTEGER NOT NULL,
                    destination_fingerprint BLOB NOT NULL,
                    pending_rows INTEGER NOT NULL,
                    pending_bytes INTEGER NOT NULL
                );
                CREATE TABLE assigned_batches (
                    sequence_id INTEGER PRIMARY KEY,
                    assigned_at_ms INTEGER NOT NULL,
                    event_count INTEGER NOT NULL,
                    payload_bytes INTEGER NOT NULL
                );",
                    )?;
                    transaction.execute(
                        "INSERT INTO spool_state(
                            singleton, spool_id, next_seq, destination_fingerprint,
                            pending_rows, pending_bytes
                         ) VALUES (1, randomblob(16), 1, ?1, 0, 0)",
                        params![destination_fingerprint],
                    )?;
                }
                1 => {
                    transaction.execute_batch(
                        "ALTER TABLE pending_events ADD COLUMN batch_seq INTEGER;
                ALTER TABLE pending_events ADD COLUMN recovered INTEGER NOT NULL DEFAULT 0;
                CREATE INDEX IF NOT EXISTS pending_events_batch
                    ON pending_events(batch_seq, id);
                CREATE TABLE spool_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    spool_id BLOB NOT NULL,
                    next_seq INTEGER NOT NULL,
                    destination_fingerprint BLOB NOT NULL,
                    pending_rows INTEGER NOT NULL,
                    pending_bytes INTEGER NOT NULL
                );
                CREATE TABLE assigned_batches (
                    sequence_id INTEGER PRIMARY KEY,
                    assigned_at_ms INTEGER NOT NULL,
                    event_count INTEGER NOT NULL,
                    payload_bytes INTEGER NOT NULL
                );",
                    )?;
                    let (rows, bytes) = actual_pending_totals(&transaction)?;
                    transaction.execute(
                        "INSERT INTO spool_state(
                            singleton, spool_id, next_seq, destination_fingerprint,
                            pending_rows, pending_bytes
                         ) VALUES (1, randomblob(16), 1, ?1, ?2, ?3)",
                        params![
                            destination_fingerprint,
                            i64::try_from(rows).unwrap_or(i64::MAX),
                            i64::try_from(bytes).unwrap_or(i64::MAX)
                        ],
                    )?;
                }
                2 | 3 => {
                    if version == 2 {
                        transaction.execute(
                            "ALTER TABLE pending_events
                             ADD COLUMN recovered INTEGER NOT NULL DEFAULT 0",
                            [],
                        )?;
                    }
                    transaction.execute_batch(
                        "ALTER TABLE spool_state ADD COLUMN destination_fingerprint BLOB;
                         ALTER TABLE spool_state ADD COLUMN pending_rows INTEGER NOT NULL DEFAULT 0;
                         ALTER TABLE spool_state ADD COLUMN pending_bytes INTEGER NOT NULL DEFAULT 0;
                         ALTER TABLE assigned_batches
                            ADD COLUMN event_count INTEGER NOT NULL DEFAULT 0;
                         ALTER TABLE assigned_batches
                            ADD COLUMN payload_bytes INTEGER NOT NULL DEFAULT 0;",
                    )?;
                    let (rows, bytes) = actual_pending_totals(&transaction)?;
                    transaction.execute(
                        "UPDATE spool_state SET
                            destination_fingerprint = ?1,
                            pending_rows = ?2,
                            pending_bytes = ?3
                         WHERE singleton = 1",
                        params![
                            destination_fingerprint,
                            i64::try_from(rows).unwrap_or(i64::MAX),
                            i64::try_from(bytes).unwrap_or(i64::MAX)
                        ],
                    )?;
                    transaction.execute(
                        "UPDATE assigned_batches SET
                            event_count = (
                                SELECT COUNT(*) FROM pending_events
                                WHERE batch_seq = assigned_batches.sequence_id
                            ),
                            payload_bytes = (
                                SELECT COALESCE(SUM(payload_bytes), 0) FROM pending_events
                                WHERE batch_seq = assigned_batches.sequence_id
                            )",
                        [],
                    )?;
                }
                _ => unreachable!("schema version checked above"),
            }
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
        }
        let stored_fingerprint = connection.query_row(
            "SELECT destination_fingerprint FROM spool_state WHERE singleton = 1",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
        if stored_fingerprint != destination_fingerprint {
            return Err(PersistenceError::DestinationMismatch);
        }
        Ok(Self {
            connection,
            config,
            #[cfg(test)]
            committed_transactions: 0,
            #[cfg(test)]
            fail_group_commit: false,
        })
    }

    #[cfg(test)]
    fn insert(
        &mut self,
        metadata: &LogGroupMetadata,
        log: &Log,
    ) -> Result<InsertOutcome, PersistenceError> {
        self.insert_at(metadata, log, now_millis())
    }

    #[cfg(test)]
    fn insert_at(
        &mut self,
        metadata: &LogGroupMetadata,
        log: &Log,
        created_at_ms: i64,
    ) -> Result<InsertOutcome, PersistenceError> {
        let payload =
            encode_persisted_event(metadata, log).map_err(PersistenceError::Serialization)?;
        if payload.len() as u64 > self.config.payload_budget() {
            return Err(PersistenceError::EventTooLarge);
        }
        self.insert_group(&[PreparedInsert {
            payload,
            created_at_ms,
            acknowledgement: mpsc::sync_channel(1).0,
        }])?
        .pop()
        .expect("single insert produces one result")
    }

    fn insert_group(
        &mut self,
        commands: &[PreparedInsert],
    ) -> Result<Vec<Result<InsertOutcome, PersistenceError>>, PersistenceError> {
        let mut transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut outcomes = Vec::with_capacity(commands.len());
        for command in commands {
            let savepoint = transaction.savepoint()?;
            match Self::insert_encoded(
                &savepoint,
                &self.config,
                &command.payload,
                command.created_at_ms,
            ) {
                Ok(outcome) => {
                    savepoint.commit()?;
                    outcomes.push(Ok(outcome));
                }
                Err(PersistenceError::EventTooLarge) => {
                    outcomes.push(Err(PersistenceError::EventTooLarge));
                }
                Err(error) => return Err(error),
            }
        }
        #[cfg(test)]
        if self.fail_group_commit {
            return Err(PersistenceError::Sqlite(rusqlite::Error::InvalidQuery));
        }
        transaction.commit()?;
        #[cfg(test)]
        {
            self.committed_transactions = self.committed_transactions.saturating_add(1);
        }
        Ok(outcomes)
    }

    fn insert_encoded(
        connection: &Connection,
        config: &PersistenceConfig,
        payload: &[u8],
        created_at_ms: i64,
    ) -> Result<InsertOutcome, PersistenceError> {
        let mut outcome = InsertOutcome::default();
        let (mut pending_rows, mut pending_bytes) = state_totals(connection)?;
        if let Some(max_age) = config.max_age {
            let age_ms = i64::try_from(max_age.as_millis()).unwrap_or(i64::MAX);
            let cutoff = created_at_ms.saturating_sub(age_ms);
            let (rows, bytes) = totals_where(
                connection,
                "WHERE batch_seq IS NULL AND created_at_ms < ?1",
                params![cutoff],
            )?;
            connection
                .prepare_cached(
                    "DELETE FROM pending_events
                 WHERE batch_seq IS NULL AND created_at_ms < ?1",
                )?
                .execute(params![cutoff])?;
            outcome.evicted_age = rows;
            outcome.evicted_bytes = outcome.evicted_bytes.saturating_add(bytes);
            pending_rows = pending_rows.saturating_sub(rows);
            pending_bytes = pending_bytes.saturating_sub(bytes);
        }
        if let Some(max_events) = config.max_events {
            let required = pending_rows.saturating_add(1).saturating_sub(max_events);
            if required != 0 {
                let Some((rows, bytes)) = evict_oldest_unassigned(connection, required, 0)? else {
                    return Err(PersistenceError::EventTooLarge);
                };
                if rows < required {
                    return Err(PersistenceError::EventTooLarge);
                }
                outcome.evicted_count = outcome.evicted_count.saturating_add(rows);
                outcome.evicted_bytes = outcome.evicted_bytes.saturating_add(bytes);
                pending_rows = pending_rows.saturating_sub(rows);
                pending_bytes = pending_bytes.saturating_sub(bytes);
            }
        }
        let required_bytes = pending_bytes
            .saturating_add(payload.len() as u64)
            .saturating_sub(config.payload_budget());
        if required_bytes != 0 {
            let Some((rows, bytes)) = evict_oldest_unassigned(connection, 0, required_bytes)?
            else {
                return Err(PersistenceError::EventTooLarge);
            };
            if bytes < required_bytes {
                return Err(PersistenceError::EventTooLarge);
            }
            outcome.evicted_storage = outcome.evicted_storage.saturating_add(rows);
            outcome.evicted_bytes = outcome.evicted_bytes.saturating_add(bytes);
            pending_rows = pending_rows.saturating_sub(rows);
            pending_bytes = pending_bytes.saturating_sub(bytes);
        }
        connection
            .prepare_cached(
                "INSERT INTO pending_events(created_at_ms, payload, payload_bytes)
             VALUES (?1, ?2, ?3)",
            )?
            .execute(params![
                created_at_ms,
                payload,
                i64::try_from(payload.len()).unwrap_or(i64::MAX)
            ])?;
        outcome.pending_rows = pending_rows.saturating_add(1);
        outcome.pending_bytes = pending_bytes.saturating_add(payload.len() as u64);
        set_state_totals(connection, outcome.pending_rows, outcome.pending_bytes)?;
        Ok(outcome)
    }

    fn enforce_existing_limits(
        &mut self,
        current_time_ms: i64,
    ) -> Result<InsertOutcome, PersistenceError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut outcome = InsertOutcome::default();
        let (mut pending_rows, mut pending_bytes) = state_totals(&transaction)?;
        if let Some(max_age) = self.config.max_age {
            let age_ms = i64::try_from(max_age.as_millis()).unwrap_or(i64::MAX);
            let cutoff = current_time_ms.saturating_sub(age_ms);
            let (rows, bytes) = totals_where(
                &transaction,
                "WHERE batch_seq IS NULL AND created_at_ms < ?1",
                params![cutoff],
            )?;
            transaction.execute(
                "DELETE FROM pending_events
                 WHERE batch_seq IS NULL AND created_at_ms < ?1",
                params![cutoff],
            )?;
            outcome.evicted_age = rows;
            outcome.evicted_bytes = bytes;
            pending_rows = pending_rows.saturating_sub(rows);
            pending_bytes = pending_bytes.saturating_sub(bytes);
        }
        if let Some(max_events) = self.config.max_events {
            let required = pending_rows.saturating_sub(max_events);
            if required != 0 {
                if let Some((rows, bytes)) = evict_oldest_unassigned(&transaction, required, 0)? {
                    outcome.evicted_count = outcome.evicted_count.saturating_add(rows);
                    outcome.evicted_bytes = outcome.evicted_bytes.saturating_add(bytes);
                    pending_rows = pending_rows.saturating_sub(rows);
                    pending_bytes = pending_bytes.saturating_sub(bytes);
                }
            }
        }
        let required_bytes = pending_bytes.saturating_sub(self.config.payload_budget());
        if required_bytes != 0 {
            if let Some((rows, bytes)) = evict_oldest_unassigned(&transaction, 0, required_bytes)? {
                outcome.evicted_storage = outcome.evicted_storage.saturating_add(rows);
                outcome.evicted_bytes = outcome.evicted_bytes.saturating_add(bytes);
                pending_rows = pending_rows.saturating_sub(rows);
                pending_bytes = pending_bytes.saturating_sub(bytes);
            }
        }
        outcome.pending_rows = pending_rows;
        outcome.pending_bytes = pending_bytes;
        set_state_totals(&transaction, pending_rows, pending_bytes)?;
        transaction.commit()?;
        Ok(outcome)
    }

    #[cfg(test)]
    fn pending_totals(&self) -> Result<(u64, u64), PersistenceError> {
        state_totals(&self.connection).map_err(PersistenceError::Sqlite)
    }

    fn recover_pending(&mut self) -> Result<(u64, u64), PersistenceError> {
        let mut statement = self
            .connection
            .prepare("SELECT payload FROM pending_events ORDER BY id")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let payload = row.get::<_, Vec<u8>>(0)?;
            decode_persisted_event(&payload).map_err(PersistenceError::Serialization)?;
        }
        drop(rows);
        drop(statement);
        let actual = actual_pending_totals(&self.connection)?;
        let stored = state_totals(&self.connection)?;
        if actual != stored {
            return Err(PersistenceError::InconsistentState);
        }
        self.connection
            .execute("UPDATE pending_events SET recovered = 1", [])?;
        Ok(stored)
    }

    fn next_batch(
        &mut self,
        max_count: usize,
        max_bytes: usize,
        force: bool,
    ) -> Result<Option<PersistentBatch>, PersistenceError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT sequence_id FROM assigned_batches ORDER BY sequence_id LIMIT 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let sequence_id = if let Some(sequence_id) = existing {
            sequence_id.max(0) as u64
        } else {
            let Some((ids, metadata, logs, recovered_count, persisted_bytes, boundary_reached)) =
                select_unassigned_batch(&transaction, max_count.max(1), max_bytes.max(1))?
            else {
                transaction.commit()?;
                return Ok(None);
            };
            if !force && recovered_count == 0 && !boundary_reached {
                transaction.commit()?;
                return Ok(None);
            }
            let next_seq = transaction.query_row(
                "SELECT next_seq FROM spool_state WHERE singleton = 1",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let sequence_id = next_seq.max(1) as u64;
            transaction.execute(
                "INSERT INTO assigned_batches(
                    sequence_id, assigned_at_ms, event_count, payload_bytes
                 ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    i64::try_from(sequence_id).unwrap_or(i64::MAX),
                    now_millis(),
                    i64::try_from(logs.len()).unwrap_or(i64::MAX),
                    i64::try_from(persisted_bytes).unwrap_or(i64::MAX)
                ],
            )?;
            {
                let mut assign = transaction.prepare_cached(
                    "UPDATE pending_events SET batch_seq = ?1
                     WHERE id = ?2 AND batch_seq IS NULL",
                )?;
                for id in ids {
                    assign.execute(params![i64::try_from(sequence_id).unwrap_or(i64::MAX), id])?;
                }
            }
            transaction.execute(
                "UPDATE spool_state SET next_seq = ?1 WHERE singleton = 1",
                params![i64::try_from(sequence_id.saturating_add(1)).unwrap_or(i64::MAX)],
            )?;
            let spool_id = spool_id(&transaction)?;
            transaction.commit()?;
            return Ok(Some(PersistentBatch {
                sequence_id,
                spool_id,
                metadata,
                logs,
                recovered_count,
            }));
        };
        let (metadata, logs, recovered_count) = load_assigned_batch(&transaction, sequence_id)?;
        let spool_id = spool_id(&transaction)?;
        transaction.commit()?;
        Ok(Some(PersistentBatch {
            sequence_id,
            spool_id,
            metadata,
            logs,
            recovered_count,
        }))
    }

    fn acknowledge(&mut self, sequence_id: u64) -> Result<InsertOutcome, PersistenceError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let sequence_id = i64::try_from(sequence_id).unwrap_or(i64::MAX);
        let acknowledged = transaction
            .query_row(
                "SELECT event_count, payload_bytes FROM assigned_batches
                 WHERE sequence_id = ?1",
                params![sequence_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?.max(0) as u64,
                        row.get::<_, i64>(1)?.max(0) as u64,
                    ))
                },
            )
            .optional()?
            .unwrap_or_default();
        transaction.execute(
            "DELETE FROM pending_events WHERE batch_seq = ?1",
            params![sequence_id],
        )?;
        transaction.execute(
            "DELETE FROM assigned_batches WHERE sequence_id = ?1",
            params![sequence_id],
        )?;
        let totals = state_totals(&transaction)?;
        let pending_rows = totals.0.saturating_sub(acknowledged.0);
        let pending_bytes = totals.1.saturating_sub(acknowledged.1);
        set_state_totals(&transaction, pending_rows, pending_bytes)?;
        transaction.commit()?;
        Ok(InsertOutcome {
            pending_rows,
            pending_bytes,
            ..InsertOutcome::default()
        })
    }

    #[cfg(test)]
    fn load_pending(&self) -> Result<Vec<(LogGroupMetadata, Log)>, PersistenceError> {
        let mut statement = self
            .connection
            .prepare("SELECT payload FROM pending_events ORDER BY id")?;
        let payloads = statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        payloads
            .iter()
            .map(|payload| decode_persisted_event(payload).map_err(PersistenceError::Serialization))
            .collect()
    }
}

type DecodedBatch = (Vec<i64>, LogGroupMetadata, Vec<Log>, usize, u64, bool);

fn select_unassigned_batch(
    transaction: &Transaction<'_>,
    max_count: usize,
    max_bytes: usize,
) -> Result<Option<DecodedBatch>, PersistenceError> {
    let mut statement = transaction.prepare(
        "SELECT id, payload, recovered, payload_bytes FROM pending_events
         WHERE batch_seq IS NULL ORDER BY id LIMIT ?1",
    )?;
    let query_limit = max_count.saturating_add(1);
    let mut rows = statement.query(params![i64::try_from(query_limit).unwrap_or(i64::MAX)])?;
    let mut ids = Vec::with_capacity(max_count.min(1024));
    let mut logs = Vec::with_capacity(max_count.min(1024));
    let mut metadata = None;
    let mut encoded_len = 0usize;
    let mut recovered_count = 0usize;
    let mut persisted_bytes = 0u64;
    let mut boundary_reached = false;
    while let Some(row) = rows.next()? {
        let id = row.get::<_, i64>(0)?;
        let payload = row.get::<_, Vec<u8>>(1)?;
        let recovered = row.get::<_, i64>(2)? != 0;
        let payload_bytes = row.get::<_, i64>(3)?.max(0) as u64;
        let (row_metadata, log) =
            decode_persisted_event(&payload).map_err(PersistenceError::Serialization)?;
        if let Some(first_metadata) = &metadata {
            if first_metadata != &row_metadata {
                boundary_reached = true;
                break;
            }
        } else {
            encoded_len = calc_log_group_metadata_encoded_len(&row_metadata);
            metadata = Some(row_metadata);
        }
        let added = calc_log_group_log_encoded_len(&log);
        if !logs.is_empty() && encoded_len.saturating_add(added) > max_bytes {
            boundary_reached = true;
            break;
        }
        encoded_len = encoded_len.saturating_add(added);
        ids.push(id);
        logs.push(log);
        recovered_count = recovered_count.saturating_add(usize::from(recovered));
        persisted_bytes = persisted_bytes.saturating_add(payload_bytes);
        if logs.len() >= max_count || encoded_len >= max_bytes {
            boundary_reached = true;
            break;
        }
    }
    Ok(metadata.map(|metadata| {
        (
            ids,
            metadata,
            logs,
            recovered_count,
            persisted_bytes,
            boundary_reached,
        )
    }))
}

fn load_assigned_batch(
    transaction: &Transaction<'_>,
    sequence_id: u64,
) -> Result<(LogGroupMetadata, Vec<Log>, usize), PersistenceError> {
    let mut statement = transaction.prepare(
        "SELECT payload, recovered FROM pending_events WHERE batch_seq = ?1 ORDER BY id",
    )?;
    let mut rows = statement.query(params![i64::try_from(sequence_id).unwrap_or(i64::MAX)])?;
    let mut metadata = None;
    let mut logs = Vec::new();
    let mut recovered_count = 0usize;
    while let Some(row) = rows.next()? {
        let payload = row.get::<_, Vec<u8>>(0)?;
        let recovered = row.get::<_, i64>(1)? != 0;
        let (row_metadata, log) =
            decode_persisted_event(&payload).map_err(PersistenceError::Serialization)?;
        if metadata
            .as_ref()
            .is_some_and(|value| value != &row_metadata)
        {
            return Err(PersistenceError::Serialization(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "assigned batch metadata changed",
            )));
        }
        metadata.get_or_insert(row_metadata);
        logs.push(log);
        recovered_count = recovered_count.saturating_add(usize::from(recovered));
    }
    let metadata = metadata.ok_or_else(|| {
        PersistenceError::Serialization(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "assigned batch has no events",
        ))
    })?;
    Ok((metadata, logs, recovered_count))
}

fn spool_id(connection: &Connection) -> Result<Box<[u8]>, PersistenceError> {
    connection
        .query_row(
            "SELECT spool_id FROM spool_state WHERE singleton = 1",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map(Vec::into_boxed_slice)
        .map_err(PersistenceError::Sqlite)
}

fn state_totals(connection: &Connection) -> rusqlite::Result<(u64, u64)> {
    connection
        .prepare_cached("SELECT pending_rows, pending_bytes FROM spool_state WHERE singleton = 1")?
        .query_row([], |row| {
            Ok((
                row.get::<_, i64>(0)?.max(0) as u64,
                row.get::<_, i64>(1)?.max(0) as u64,
            ))
        })
}

fn set_state_totals(
    connection: &Connection,
    pending_rows: u64,
    pending_bytes: u64,
) -> rusqlite::Result<()> {
    connection
        .prepare_cached(
            "UPDATE spool_state SET pending_rows = ?1, pending_bytes = ?2
         WHERE singleton = 1",
        )?
        .execute(params![
            i64::try_from(pending_rows).unwrap_or(i64::MAX),
            i64::try_from(pending_bytes).unwrap_or(i64::MAX)
        ])?;
    Ok(())
}

fn actual_pending_totals(connection: &Connection) -> rusqlite::Result<(u64, u64)> {
    connection.query_row(
        "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) FROM pending_events",
        [],
        |row| {
            let rows = row.get::<_, i64>(0)?;
            let bytes = row.get::<_, i64>(1)?;
            Ok((rows.max(0) as u64, bytes.max(0) as u64))
        },
    )
}

fn totals_where(
    connection: &Connection,
    suffix: &str,
    parameters: impl rusqlite::Params,
) -> rusqlite::Result<(u64, u64)> {
    connection.query_row(
        &format!("SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) FROM pending_events {suffix}"),
        parameters,
        |row| {
            let rows = row.get::<_, i64>(0)?;
            let bytes = row.get::<_, i64>(1)?;
            Ok((rows.max(0) as u64, bytes.max(0) as u64))
        },
    )
}

fn evict_oldest_unassigned(
    connection: &Connection,
    required_rows: u64,
    required_bytes: u64,
) -> rusqlite::Result<Option<(u64, u64)>> {
    let mut evicted_rows = 0u64;
    let mut evicted_bytes = 0u64;
    loop {
        if evicted_rows >= required_rows && evicted_bytes >= required_bytes {
            return Ok(Some((evicted_rows, evicted_bytes)));
        }
        let mut statement = connection.prepare_cached(
            "SELECT id, payload_bytes FROM pending_events
             WHERE batch_seq IS NULL ORDER BY id LIMIT ?1",
        )?;
        let mut rows = statement.query(params![EVICTION_CHUNK_ROWS])?;
        let mut chunk_rows = 0u64;
        let mut chunk_bytes = 0u64;
        let mut last_id = None;
        while let Some(row) = rows.next()? {
            last_id = Some(row.get::<_, i64>(0)?);
            chunk_rows = chunk_rows.saturating_add(1);
            chunk_bytes = chunk_bytes.saturating_add(row.get::<_, i64>(1)?.max(0) as u64);
            if evicted_rows.saturating_add(chunk_rows) >= required_rows
                && evicted_bytes.saturating_add(chunk_bytes) >= required_bytes
            {
                break;
            }
        }
        drop(rows);
        drop(statement);
        let Some(last_id) = last_id else {
            return Ok((evicted_rows != 0).then_some((evicted_rows, evicted_bytes)));
        };
        connection
            .prepare_cached(
                "DELETE FROM pending_events
             WHERE batch_seq IS NULL AND id <= ?1",
            )?
            .execute(params![last_id])?;
        evicted_rows = evicted_rows.saturating_add(chunk_rows);
        evicted_bytes = evicted_bytes.saturating_add(chunk_bytes);
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(unix)]
fn secure_spool_file(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn secure_spool_file(path: &Path) -> Result<(), std::io::Error> {
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MayStaticKey;

    fn temp_path(name: &str) -> PathBuf {
        let unique = format!(
            "aliyun-sls-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        );
        std::env::temp_dir().join(unique).join("spool.sqlite3")
    }

    fn event(value: &str) -> (LogGroupMetadata, Log) {
        (
            LogGroupMetadata::new()
                .with_topic("topic")
                .with_source("source")
                .with_tag(MayStaticKey::from_static("service"), "test"),
            Log::new(7, Some(9)).with(MayStaticKey::from_static("message"), value),
        )
    }

    fn assert_totals_consistent(store: &Store) {
        assert_eq!(
            state_totals(&store.connection).expect("state totals"),
            actual_pending_totals(&store.connection).expect("actual totals")
        );
    }

    #[test]
    fn config_rejects_zero_limits() {
        assert!(PersistenceConfig::new(0).validate().is_err());
        assert!(PersistenceConfig::new(1).max_events(0).validate().is_err());
        assert!(
            PersistenceConfig::new(1)
                .max_age(Duration::ZERO)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn default_path_is_next_to_current_executable() {
        let executable = std::env::current_exe().expect("current executable");
        let expected_name = format!(
            "{}.sls-spool.sqlite3",
            executable
                .file_name()
                .and_then(|name| name.to_str())
                .expect("executable name")
        );
        let path = PersistenceConfig::new(1)
            .resolve_path()
            .expect("default path");

        assert_eq!(path.parent(), executable.parent());
        assert_eq!(
            path.file_name().and_then(|name| name.to_str()),
            Some(expected_name.as_str())
        );
    }

    #[test]
    fn spool_is_bound_to_exact_destination() {
        let path = temp_path("destination");
        let config = PersistenceConfig::new(1024 * 1024).path(path.clone());
        drop(
            Store::open_for_destination(config.clone(), &path, b"endpoint/project/logstore-a")
                .expect("create destination-bound spool"),
        );

        assert!(matches!(
            Store::open_for_destination(config, &path, b"endpoint/project/logstore-b"),
            Err(PersistenceError::DestinationMismatch)
        ));
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn v1_and_v2_migrations_are_atomic_and_initialize_state() {
        for version in [1, 2] {
            let path = temp_path(&format!("migration-v{version}"));
            fs::create_dir_all(path.parent().expect("temporary parent")).expect("create parent");
            let connection = Connection::open(&path).expect("create legacy database");
            connection
                .execute_batch(
                    "CREATE TABLE pending_events (
                        id INTEGER PRIMARY KEY,
                        created_at_ms INTEGER NOT NULL,
                        payload BLOB NOT NULL,
                        payload_bytes INTEGER NOT NULL
                    );",
                )
                .expect("create v1 events");
            if version == 2 {
                connection
                    .execute_batch(
                        "ALTER TABLE pending_events ADD COLUMN batch_seq INTEGER;
                         CREATE TABLE spool_state (
                            singleton INTEGER PRIMARY KEY,
                            spool_id BLOB NOT NULL,
                            next_seq INTEGER NOT NULL
                         );
                         INSERT INTO spool_state VALUES (1, randomblob(16), 1);
                         CREATE TABLE assigned_batches (
                            sequence_id INTEGER PRIMARY KEY,
                            assigned_at_ms INTEGER NOT NULL
                         );",
                    )
                    .expect("create v2 state");
            }
            connection
                .execute(
                    "INSERT INTO pending_events(created_at_ms, payload, payload_bytes)
                     VALUES (1, x'00', 7)",
                    [],
                )
                .expect("insert legacy row");
            if version == 2 {
                connection
                    .execute("UPDATE pending_events SET batch_seq = 5", [])
                    .expect("assign legacy row");
                connection
                    .execute("INSERT INTO assigned_batches VALUES (5, 1)", [])
                    .expect("insert legacy batch");
            }
            connection
                .pragma_update(None, "user_version", version)
                .expect("set legacy version");
            drop(connection);

            let store = Store::open_for_destination(
                PersistenceConfig::new(1024 * 1024).path(path.clone()),
                &path,
                b"migration-destination",
            )
            .expect("migrate legacy database");
            let migrated: i64 = store
                .connection
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .expect("read migrated version");
            assert_eq!(migrated, SCHEMA_VERSION);
            assert_eq!(state_totals(&store.connection).expect("totals"), (1, 7));
            if version == 2 {
                let assigned: (i64, i64) = store
                    .connection
                    .query_row(
                        "SELECT event_count, payload_bytes FROM assigned_batches
                         WHERE sequence_id = 5",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .expect("read migrated assignment totals");
                assert_eq!(assigned, (1, 7));
            }
            assert_totals_consistent(&store);
            drop(store);
            let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
        }
    }

    #[test]
    fn partially_applicable_migration_rolls_back_without_version_change() {
        let path = temp_path("partial-migration");
        fs::create_dir_all(path.parent().expect("temporary parent")).expect("create parent");
        let connection = Connection::open(&path).expect("create database");
        connection
            .execute_batch(
                "CREATE TABLE pending_events (
                    id INTEGER PRIMARY KEY,
                    created_at_ms INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    payload_bytes INTEGER NOT NULL,
                    batch_seq INTEGER
                 );
                 PRAGMA user_version = 1;",
            )
            .expect("create inconsistent v1 schema");
        drop(connection);

        assert!(matches!(
            Store::open_for_destination(
                PersistenceConfig::new(1024 * 1024).path(path.clone()),
                &path,
                b"destination"
            ),
            Err(PersistenceError::Sqlite(_))
        ));
        let connection = Connection::open(&path).expect("reopen failed migration");
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read version");
        assert_eq!(version, 1);
        let state_exists: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'spool_state'",
                [],
                |row| row.get(0),
            )
            .expect("check rollback");
        assert_eq!(state_exists, 0);
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn bounded_command_queue_reports_full_without_waiting() {
        let (sender, receiver) = async_channel::bounded(1);
        let (_wake_sender, wake_receiver) = async_channel::bounded(1);
        let persistence = PersistenceSender {
            sender,
            wake_receiver,
        };
        let (metadata, log) = event("queued");
        let (acknowledgement, _result) = mpsc::sync_channel(1);
        persistence
            .sender
            .try_send(Command::Insert {
                metadata: Arc::new(metadata),
                log: Box::new(log),
                acknowledgement,
            })
            .expect("first command fills queue");
        let state = State::default();
        let (metadata, log) = event("rejected");

        assert!(matches!(
            persistence.commit(Arc::new(metadata), log, &state),
            Err(AdmissionFailure::Full)
        ));
        drop(receiver);
    }

    fn queue_insert(
        sender: &async_channel::Sender<Command>,
        value: &str,
    ) -> mpsc::Receiver<Result<(), PersistenceError>> {
        let (metadata, log) = event(value);
        let (acknowledgement, result) = mpsc::sync_channel(1);
        sender
            .send_blocking(Command::Insert {
                metadata: Arc::new(metadata),
                log: Box::new(log),
                acknowledgement,
            })
            .expect("queue insert");
        result
    }

    fn run_queued_commands(store: &mut Store, receiver: async_channel::Receiver<Command>) {
        let (wake_sender, _wake_receiver) = async_channel::bounded(1);
        writer_loop(store, receiver, wake_sender, &State::default());
    }

    #[test]
    fn consecutive_inserts_share_commits_and_are_visible_before_ack() {
        let path = temp_path("group-commit");
        let mut store = Store::open(
            PersistenceConfig::new(8 * 1024 * 1024).path(path.clone()),
            &path,
        )
        .expect("open store");
        let (sender, receiver) = async_channel::bounded(64);
        let acknowledgements = (0..20)
            .map(|index| queue_insert(&sender, &format!("event-{index}")))
            .collect::<Vec<_>>();
        drop(sender);

        run_queued_commands(&mut store, receiver);

        assert_eq!(store.committed_transactions, 1);
        let observer = Connection::open(&path).expect("open observer");
        for (index, acknowledgement) in acknowledgements.into_iter().enumerate() {
            acknowledgement
                .recv()
                .expect("writer response")
                .expect("committed insert");
            let visible: i64 = observer
                .query_row("SELECT COUNT(*) FROM pending_events", [], |row| row.get(0))
                .expect("count committed rows");
            assert_eq!(visible, 20, "ack {index} preceded the shared commit");
        }
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn too_large_peer_isolated_without_blocking_valid_group() {
        let path = temp_path("group-invalid-peer");
        let mut store = Store::open(
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
            &path,
        )
        .expect("open store");
        let (sender, receiver) = async_channel::bounded(8);
        let first = queue_insert(&sender, "first");
        let oversized = queue_insert(&sender, &"x".repeat(1024 * 1024));
        let second = queue_insert(&sender, "second");
        drop(sender);

        run_queued_commands(&mut store, receiver);

        first.recv().expect("first response").expect("first commit");
        assert!(matches!(
            oversized.recv().expect("oversized response"),
            Err(PersistenceError::EventTooLarge)
        ));
        second
            .recv()
            .expect("second response")
            .expect("second commit");
        assert_eq!(store.committed_transactions, 1);
        assert_eq!(
            store
                .load_pending()
                .expect("load valid peers")
                .into_iter()
                .map(|(_, log)| log)
                .collect::<Vec<_>>(),
            vec![event("first").1, event("second").1]
        );
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn non_insert_command_is_not_overtaken_by_group_drain() {
        let path = temp_path("group-ordering");
        let mut store = Store::open(
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
            &path,
        )
        .expect("open store");
        let (sender, receiver) = async_channel::bounded(8);
        let first = queue_insert(&sender, "first");
        let (batch_sender, batch_result) = async_channel::bounded(1);
        sender
            .send_blocking(Command::NextBatch {
                max_count: 10,
                max_bytes: usize::MAX,
                force: true,
                acknowledgement: batch_sender,
            })
            .expect("queue next batch");
        let second = queue_insert(&sender, "second");
        drop(sender);

        run_queued_commands(&mut store, receiver);

        first.recv().expect("first response").expect("first commit");
        let batch = batch_result
            .recv_blocking()
            .expect("batch response")
            .expect("batch query")
            .expect("assigned batch");
        assert_eq!(batch.logs, vec![event("first").1]);
        second
            .recv()
            .expect("second response")
            .expect("second commit");
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn fatal_group_failure_rolls_back_every_insert_without_acknowledging_success() {
        let path = temp_path("group-rollback");
        let mut store = Store::open(
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
            &path,
        )
        .expect("open store");
        store.fail_group_commit = true;
        let (sender, receiver) = async_channel::bounded(8);
        let first = queue_insert(&sender, "first");
        let second = queue_insert(&sender, "second");
        drop(sender);

        run_queued_commands(&mut store, receiver);

        assert!(first.recv().expect("first response").is_err());
        assert!(second.recv().expect("second response").is_err());
        assert_eq!(store.committed_transactions, 0);
        assert_eq!(
            actual_pending_totals(&store.connection).expect("totals"),
            (0, 0)
        );
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn group_commit_count_and_byte_limits_bound_transactions() {
        let count_path = temp_path("group-count-bound");
        let mut count_store = Store::open(
            PersistenceConfig::new(16 * 1024 * 1024).path(count_path.clone()),
            &count_path,
        )
        .expect("open count store");
        let (sender, receiver) = async_channel::bounded(64);
        let acknowledgements = (0..MAX_GROUP_COMMIT_EVENTS + 1)
            .map(|index| queue_insert(&sender, &format!("event-{index}")))
            .collect::<Vec<_>>();
        drop(sender);
        run_queued_commands(&mut count_store, receiver);
        assert_eq!(count_store.committed_transactions, 2);
        assert!(
            acknowledgements
                .into_iter()
                .all(|ack| ack.recv().expect("response").is_ok())
        );

        let byte_path = temp_path("group-byte-bound");
        let mut byte_store = Store::open(
            PersistenceConfig::new(8 * 1024 * 1024).path(byte_path.clone()),
            &byte_path,
        )
        .expect("open byte store");
        let (sender, receiver) = async_channel::bounded(4);
        let first = queue_insert(&sender, &"a".repeat(MAX_GROUP_COMMIT_BYTES / 2 + 1));
        let second = queue_insert(&sender, &"b".repeat(MAX_GROUP_COMMIT_BYTES / 2 + 1));
        drop(sender);
        let state = State::default();
        state.stats.queue_depth.store(2, Ordering::Relaxed);
        let (wake_sender, _wake_receiver) = async_channel::bounded(1);
        writer_loop(&mut byte_store, receiver, wake_sender, &state);
        assert_eq!(byte_store.committed_transactions, 2);
        assert_eq!(state.stats.queue_depth.load(Ordering::Relaxed), 0);
        assert!(first.recv().expect("first response").is_ok());
        assert!(second.recv().expect("second response").is_ok());

        for path in [count_path, byte_path] {
            let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
        }
    }

    #[test]
    fn wal_uses_bounded_less_frequent_checkpoint_policy() {
        let path = temp_path("checkpoint-policy");
        let storage_budget = 128 * 1024;
        let store = Store::open(
            PersistenceConfig::new(storage_budget).path(path.clone()),
            &path,
        )
        .expect("open store");
        let pages: i64 = store
            .connection
            .pragma_query_value(None, "wal_autocheckpoint", |row| row.get(0))
            .expect("read WAL checkpoint policy");
        let page_size: i64 = store
            .connection
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .expect("read page size");

        assert_eq!(
            pages,
            (storage_budget / 4 / page_size as u64)
                .max(1)
                .min(MAX_WAL_AUTOCHECKPOINT_BYTES / page_size as u64) as i64
        );
        assert!(pages.saturating_mul(page_size) <= storage_budget as i64 / 4);
        drop(store);
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn event_roundtrips_and_recovers_after_reopen() {
        let path = temp_path("roundtrip");
        let config = PersistenceConfig::new(1024 * 1024).path(path.clone());
        let expected = event("hello");
        {
            let mut store = Store::open(config.clone(), &path).expect("open store");
            store
                .insert(&expected.0, &expected.1)
                .expect("insert event");
            assert_eq!(
                store.load_pending().expect("decode event"),
                vec![expected.clone()]
            );
        }
        let store = Store::open(config, &path).expect("reopen store");
        assert_eq!(store.load_pending().expect("recover event"), vec![expected]);
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn batch_assignment_is_immutable_and_sequence_is_monotonic() {
        let path = temp_path("batch-assignment");
        let config = PersistenceConfig::new(1024 * 1024).path(path.clone());
        let first = event("first");
        let second = event("second");
        let third = event("third");
        let first_batch = {
            let mut store = Store::open(config.clone(), &path).expect("open store");
            store.insert(&first.0, &first.1).expect("insert first");
            store.insert(&second.0, &second.1).expect("insert second");
            store.insert(&third.0, &third.1).expect("insert third");
            store
                .next_batch(2, usize::MAX, true)
                .expect("assign")
                .expect("batch")
        };
        assert_eq!(first_batch.sequence_id, 1);
        assert_eq!(first_batch.logs, vec![first.1.clone(), second.1.clone()]);

        let mut reopened = Store::open(config, &path).expect("reopen store");
        reopened.recover_pending().expect("mark recovered rows");
        let recovered = reopened
            .next_batch(1, 1, true)
            .expect("recover assignment")
            .expect("assigned batch");
        assert_eq!(recovered.sequence_id, first_batch.sequence_id);
        assert_eq!(recovered.spool_id, first_batch.spool_id);
        assert_eq!(recovered.logs, first_batch.logs);
        assert_eq!(recovered.recovered_count, 2);

        reopened
            .acknowledge(recovered.sequence_id)
            .expect("ack first batch");
        let next = reopened
            .next_batch(10, usize::MAX, true)
            .expect("assign next")
            .expect("next batch");
        assert_eq!(next.sequence_id, 2);
        assert_eq!(next.logs, vec![third.1]);
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn remote_ack_crash_window_retries_same_batch_until_local_ack_deletes_it() {
        let path = temp_path("remote-before-local-ack");
        let config = PersistenceConfig::new(1024 * 1024).path(path.clone());
        let expected = event("uncertain-remote-ack");
        let assigned = {
            let mut store = Store::open(config.clone(), &path).expect("open store");
            store
                .insert(&expected.0, &expected.1)
                .expect("insert event");
            store
                .next_batch(1, usize::MAX, true)
                .expect("assign batch")
                .expect("batch")
        };

        // Simulate SLS acknowledging the request and the process stopping before
        // the local SQLite acknowledgement transaction.
        let mut reopened = Store::open(config.clone(), &path).expect("reopen after remote ack");
        reopened.recover_pending().expect("recover pending batch");
        let retried = reopened
            .next_batch(1, usize::MAX, true)
            .expect("load uncertain batch")
            .expect("uncertain batch retained");
        assert_eq!(retried.sequence_id, assigned.sequence_id);
        assert_eq!(retried.spool_id, assigned.spool_id);
        assert_eq!(retried.logs, assigned.logs);

        reopened
            .acknowledge(retried.sequence_id)
            .expect("commit local acknowledgement");
        drop(reopened);

        let mut after_ack = Store::open(config, &path).expect("reopen after local ack");
        after_ack.recover_pending().expect("recover empty spool");
        assert!(
            after_ack
                .next_batch(1, usize::MAX, true)
                .expect("check empty spool")
                .is_none()
        );
        assert_eq!(after_ack.pending_totals().expect("empty totals"), (0, 0));
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn fresh_batches_wait_for_threshold_or_forced_linger_flush() {
        let path = temp_path("fresh-threshold");
        let mut store = Store::open(
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
            &path,
        )
        .expect("open store");
        let first = event("first");
        store.insert(&first.0, &first.1).expect("insert first");
        assert!(
            store
                .next_batch(2, usize::MAX, false)
                .expect("check threshold")
                .is_none()
        );

        let second = event("second");
        store.insert(&second.0, &second.1).expect("insert second");
        let full = store
            .next_batch(2, usize::MAX, false)
            .expect("assign at count")
            .expect("full batch");
        assert_eq!(full.logs.len(), 2);
        store.acknowledge(full.sequence_id).expect("ack full batch");

        let lingered = event("lingered");
        store
            .insert(&lingered.0, &lingered.1)
            .expect("insert lingered");
        assert!(
            store
                .next_batch(2, usize::MAX, false)
                .expect("before linger")
                .is_none()
        );
        assert!(
            store
                .next_batch(2, usize::MAX, true)
                .expect("linger flush")
                .is_some()
        );
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn recovered_backlog_bypasses_fresh_linger() {
        let path = temp_path("recovered-backlog");
        let config = PersistenceConfig::new(1024 * 1024).path(path.clone());
        {
            let mut store = Store::open(config.clone(), &path).expect("open store");
            let item = event("recovered");
            store.insert(&item.0, &item.1).expect("insert");
        }
        let mut reopened = Store::open(config, &path).expect("reopen store");
        reopened.recover_pending().expect("recover");
        let batch = reopened
            .next_batch(10, usize::MAX, false)
            .expect("assign recovered")
            .expect("recovered batch");
        assert_eq!(batch.recovered_count, 1);
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn acknowledgement_deletes_only_its_assigned_batch() {
        let path = temp_path("exact-ack");
        let mut store = Store::open(
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
            &path,
        )
        .expect("open store");
        let first = event("first");
        let second = event("second");
        store.insert(&first.0, &first.1).expect("insert first");
        let batch = store
            .next_batch(1, usize::MAX, true)
            .expect("assign")
            .expect("batch");
        store.insert(&second.0, &second.1).expect("insert second");

        let outcome = store.acknowledge(batch.sequence_id).expect("ack batch");

        assert_eq!(outcome.pending_rows, 1);
        assert_eq!(store.load_pending().expect("remaining"), vec![second]);
        assert_totals_consistent(&store);
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn retained_batch_prevents_next_sequence_from_overtaking() {
        let path = temp_path("no-overtake");
        let mut store = Store::open(
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
            &path,
        )
        .expect("open store");
        for value in ["first", "second"] {
            let item = event(value);
            store.insert(&item.0, &item.1).expect("insert");
        }
        let uncertain = store
            .next_batch(1, usize::MAX, true)
            .expect("assign first")
            .expect("first batch");
        let retried = store
            .next_batch(1, usize::MAX, true)
            .expect("reload first")
            .expect("same batch");
        assert_eq!(retried.sequence_id, uncertain.sequence_id);
        assert_eq!(retried.logs, uncertain.logs);

        store.acknowledge(uncertain.sequence_id).expect("ack first");
        let advanced = store
            .next_batch(1, usize::MAX, true)
            .expect("assign second")
            .expect("second batch");
        assert_eq!(advanced.sequence_id, uncertain.sequence_id + 1);
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn retention_never_evicts_an_assigned_batch() {
        let path = temp_path("assigned-retention");
        let mut store = Store::open(
            PersistenceConfig::new(1024 * 1024)
                .path(path.clone())
                .max_events(1),
            &path,
        )
        .expect("open store");
        let first = event("assigned");
        store.insert(&first.0, &first.1).expect("insert first");
        let batch = store
            .next_batch(1, usize::MAX, true)
            .expect("assign")
            .expect("batch");
        let second = event("rejected");

        assert!(matches!(
            store.insert(&second.0, &second.1),
            Err(PersistenceError::EventTooLarge)
        ));
        let retained = store
            .next_batch(1, usize::MAX, true)
            .expect("reload")
            .expect("retained batch");
        assert_eq!(retained.sequence_id, batch.sequence_id);
        assert_eq!(retained.logs, batch.logs);
        assert_eq!(store.pending_totals().expect("totals").0, 1);
        assert_totals_consistent(&store);
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }

    #[test]
    fn count_age_and_byte_limits_evict_oldest_rows() {
        let count_path = temp_path("count");
        let mut count_store = Store::open(
            PersistenceConfig::new(1024 * 1024)
                .path(count_path.clone())
                .max_events(2),
            &count_path,
        )
        .expect("open count store");
        let mut count_outcome = InsertOutcome::default();
        for value in ["one", "two", "three"] {
            let item = event(value);
            count_outcome = count_store.insert(&item.0, &item.1).expect("insert event");
        }
        assert_eq!(count_store.pending_totals().expect("count totals").0, 2);
        assert_eq!(count_outcome.evicted_count, 1);
        assert_eq!(count_outcome.evicted_storage, 0);
        assert_totals_consistent(&count_store);

        let age_path = temp_path("age");
        let mut age_store = Store::open(
            PersistenceConfig::new(1024 * 1024)
                .path(age_path.clone())
                .max_age(Duration::from_secs(1)),
            &age_path,
        )
        .expect("open age store");
        let old = event("old");
        age_store
            .insert_at(&old.0, &old.1, 1)
            .expect("insert old event");
        let fresh = event("fresh");
        let age_outcome = age_store
            .insert_at(&fresh.0, &fresh.1, 2_001)
            .expect("insert fresh event");
        assert_eq!(age_outcome.evicted_age, 1);
        assert_eq!(age_outcome.evicted_storage, 0);
        assert_eq!(
            age_store.load_pending().expect("load age rows"),
            vec![fresh]
        );
        assert_totals_consistent(&age_store);

        let byte_path = temp_path("bytes");
        let sample = event(&"x".repeat(70 * 1024));
        let budget = 128 * 1024;
        let mut byte_store = Store::open(
            PersistenceConfig::new(budget).path(byte_path.clone()),
            &byte_path,
        )
        .expect("open byte store");
        byte_store
            .insert(&sample.0, &sample.1)
            .expect("first event");
        let byte_outcome = byte_store
            .insert(&sample.0, &sample.1)
            .expect("second event");
        assert_eq!(byte_outcome.pending_rows, 1);
        assert_eq!(byte_outcome.evicted_storage, 1);
        assert!(byte_outcome.evicted_bytes > 0);
        assert!(byte_outcome.pending_bytes <= byte_store.config.payload_budget());
        assert_totals_consistent(&byte_store);

        for path in [count_path, age_path, byte_path] {
            drop(fs::remove_dir_all(path.parent().expect("temporary parent")));
        }
    }

    #[test]
    fn retention_counters_increment_dropped_rows() {
        let state = State::default();
        update_persistence_stats(
            &state,
            &InsertOutcome {
                evicted_age: 2,
                evicted_count: 3,
                evicted_storage: 4,
                evicted_bytes: 512,
                pending_rows: 5,
                pending_bytes: 1_024,
            },
        );

        assert_eq!(
            state.stats.persistence_evicted_age.load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            state
                .stats
                .persistence_evicted_count
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(
            state
                .stats
                .persistence_evicted_storage
                .load(Ordering::Relaxed),
            4
        );
        assert_eq!(state.stats.dropped.load(Ordering::Relaxed), 9);
        assert_eq!(
            state.stats.persistence_pending_rows.load(Ordering::Relaxed),
            5
        );
    }

    #[cfg(unix)]
    #[test]
    fn spool_file_permissions_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let path = temp_path("permissions");
        let _store = Store::open(
            PersistenceConfig::new(1024 * 1024).path(path.clone()),
            &path,
        )
        .expect("open store");
        assert_eq!(
            fs::metadata(&path)
                .expect("spool metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let _ = fs::remove_dir_all(path.parent().expect("temporary parent"));
    }
}
