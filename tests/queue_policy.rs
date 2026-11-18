use std::io;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use sawmill::{Appender, Builder, LevelFilter, Record};

const TIMEOUT: Duration = Duration::from_secs(5);

struct PausingAppender {
    messages: Sender<String>,
    resume: Receiver<()>,
}

impl Appender for PausingAppender {
    fn append(&mut self, record: &Record) -> io::Result<()> {
        self.messages
            .send(record.message.to_string())
            .map_err(io::Error::other)?;
        if record.message == "pause" {
            self.resume
                .recv_timeout(TIMEOUT)
                .map_err(io::Error::other)?;
        } else if record.message == "fail append" {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "append failed"));
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("flush failed"))
    }
}

#[test]
fn full_queue_drops_info_but_warn_waits_by_default() {
    let (messages, recorded) = mpsc::channel();
    let (release_worker, resume) = mpsc::channel();
    let guard = Builder::new(LevelFilter::Info)
        .with_queue_capacity(1)
        .init(PausingAppender { messages, resume })
        .unwrap();
    let metrics = guard.metrics();

    log::warn!("pause");
    assert_eq!(recorded.recv_timeout(TIMEOUT).unwrap(), "pause");
    log::info!("fail append");
    log::info!("drop this record");
    assert_eq!(metrics.queue_full_drops(), 1);
    assert_eq!(metrics.append_errors(), 0);

    let (starting, started) = mpsc::channel();
    let (returning, returned) = mpsc::channel();
    let producer = thread::spawn(move || {
        starting.send(()).unwrap();
        log::warn!("wait for space");
        returning.send(()).unwrap();
    });
    started.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(
        returned.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );

    release_worker.send(()).unwrap();
    returned.recv_timeout(TIMEOUT).unwrap();
    producer.join().unwrap();
    assert_eq!(
        guard.shutdown().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(metrics.queue_full_drops(), 1);
    assert_eq!(metrics.append_errors(), 1);
    assert_eq!(
        recorded.into_iter().collect::<Vec<_>>(),
        ["fail append", "wait for space"]
    );
}
