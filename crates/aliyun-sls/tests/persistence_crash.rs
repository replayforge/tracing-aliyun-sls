#![cfg(all(feature = "persist", not(target_family = "wasm")))]

use aliyun_sls::{
    Log, LogGroupMetadata, MayStaticKey, SlsClient,
    reporter::{PersistenceConfig, ReportResult, Reporter},
};
use std::{
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const CHILD_PATH_ENV: &str = "ALIYUN_SLS_PERSISTENCE_CRASH_CHILD_PATH";
const CHILD_READY: &str = "durable-row-committed";

fn client() -> SlsClient {
    SlsClient::builder()
        .access_key("test-key")
        .access_secret("test-secret")
        .expect("valid test secret")
        .endpoint("example.invalid")
        .project("persistence-test")
        .logstore("crash-recovery")
        .enable_trace(false)
        .build()
        .expect("complete test client")
}

fn spool_path() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir()
        .join(format!(
            "aliyun-sls-crash-recovery-{}-{unique}",
            std::process::id()
        ))
        .join("spool.sqlite3")
}

fn persistent_reporter(path: &Path) -> Reporter {
    Reporter::builder(client())
        .build_with_persistence(PersistenceConfig::new(1024 * 1024).path(path.to_path_buf()))
        .expect("build persistent reporter")
}

#[test]
fn persistent_child_entry() {
    let Some(path) = std::env::var_os(CHILD_PATH_ENV).map(PathBuf::from) else {
        return;
    };
    let reporter = persistent_reporter(&path);
    let metadata = Arc::new(LogGroupMetadata::new().with_topic("crash-test"));
    let log = Log::new(1, None).with(MayStaticKey::from_static("message"), "survives kill");
    assert_eq!(reporter.try_report(metadata, log), ReportResult::Accepted);
    println!("{CHILD_READY}");
    std::io::stdout().flush().expect("flush child readiness");
    std::thread::park();
}

#[test]
fn killed_process_row_is_recovered_by_reopened_reporter() {
    let path = spool_path();
    let mut child = Command::new(std::env::current_exe().expect("integration test executable"))
        .arg("--exact")
        .arg("persistent_child_entry")
        .arg("--nocapture")
        .env(CHILD_PATH_ENV, &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn persistence child");
    let stdout = child.stdout.take().expect("capture child stdout");
    let mut lines = BufReader::new(stdout).lines();
    let ready = lines
        .find_map(|line| {
            let line = line.expect("read child stdout");
            (line.contains(CHILD_READY)).then_some(line)
        })
        .expect("child confirms durable commit");
    assert!(ready.contains(CHILD_READY));

    child.kill().expect("kill child without graceful shutdown");
    let _ = child.wait().expect("reap killed child");

    let reopened = persistent_reporter(&path);
    let stats = reopened.stats();
    assert_eq!(stats.persistence_recovered_rows, 1);
    assert_eq!(stats.persistence_pending_rows, 1);
    drop(reopened);
    let _ = std::fs::remove_dir_all(path.parent().expect("temporary parent"));
}
