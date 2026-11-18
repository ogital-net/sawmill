use std::io;
use std::panic::{self, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use sawmill::{Appender, Builder, Level, LevelFilter, Record, set_max_level};

const TIMEOUT: Duration = Duration::from_secs(5);
static PREVIOUS_CALLS: AtomicUsize = AtomicUsize::new(0);

struct RecordingAppender {
    records: Sender<Record>,
    resume: Receiver<()>,
}

impl Appender for RecordingAppender {
    fn append(&mut self, record: &Record) -> io::Result<()> {
        self.records
            .send(Record {
                level: record.level,
                target: record.target.clone(),
                message: record.message.clone(),
                module_path: record.module_path.clone(),
                file: record.file.clone(),
                line: record.line,
                timestamp: record.timestamp,
            })
            .map_err(io::Error::other)?;
        if record.message == "hold worker" {
            self.resume
                .recv_timeout(TIMEOUT)
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
}

#[test]
fn panic_hook_logs_when_possible_and_always_chains() {
    panic::set_hook(Box::new(|_| {
        PREVIOUS_CALLS.fetch_add(1, Ordering::Relaxed);
    }));

    let (records, recorded) = mpsc::channel();
    let (release_worker, resume) = mpsc::channel();
    let guard = Builder::new(LevelFilter::Info)
        .with_panic_hook()
        .with_queue_capacity(1)
        .with_blocking_level(Level::Trace)
        .with_time()
        .init(RecordingAppender { records, resume })
        .unwrap();
    let metrics = guard.metrics();

    let line = line!() + 1;
    assert!(catch_unwind(|| panic::panic_any(String::from("caught panic"))).is_err());
    let record = recorded.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(record.level, Level::Error);
    assert_eq!(record.target, "panic");
    assert_eq!(record.message, "caught panic");
    assert_eq!(record.file.as_deref(), Some(file!()));
    assert_eq!(record.line, Some(line));
    assert!(record.timestamp.is_some());
    assert_eq!(PREVIOUS_CALLS.load(Ordering::Relaxed), 1);

    set_max_level(LevelFilter::Off);
    assert!(catch_unwind(|| panic!("filtered panic")).is_err());
    log::logger().flush();
    assert!(matches!(
        recorded.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(PREVIOUS_CALLS.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.queue_full_drops(), 0);
    set_max_level(LevelFilter::Info);

    log::info!("hold worker");
    assert_eq!(
        recorded.recv_timeout(TIMEOUT).unwrap().message,
        "hold worker"
    );
    log::info!("fill queue");
    assert!(catch_unwind(|| panic!("queue is full")).is_err());
    assert_eq!(PREVIOUS_CALLS.load(Ordering::Relaxed), 3);
    assert_eq!(metrics.queue_full_drops(), 1);
    release_worker.send(()).unwrap();
    guard.shutdown().unwrap();
    assert_eq!(recorded.try_recv().unwrap().message, "fill queue");
    assert!(matches!(
        recorded.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));

    assert!(catch_unwind(|| panic!("after shutdown")).is_err());
    assert_eq!(PREVIOUS_CALLS.load(Ordering::Relaxed), 4);
    assert_eq!(metrics.queue_full_drops(), 1);
}
