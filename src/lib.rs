#![doc = include_str!("../README.md")]

use std::borrow::Cow;
use std::fmt;
use std::io::{self, Write};
use std::panic::{self, PanicHookInfo};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

pub use log::{Level, LevelFilter};
use log::{Log, Metadata};

const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// A log entry that can outlive the call that produced it.
///
/// The message is formatted before queueing. Optional structured key-value
/// fields from the `log` facade are not captured. See [`Appender`] for
/// an example of formatting a record.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Record {
    /// Severity of the log entry.
    pub level: Level,
    /// Target supplied by the caller, typically the module path.
    pub target: Cow<'static, str>,
    /// Rendered message without appender-specific formatting.
    pub message: Cow<'static, str>,
    /// Module that emitted the record, if supplied by the caller.
    pub module_path: Option<Cow<'static, str>>,
    /// Source file, if supplied by the caller.
    pub file: Option<Cow<'static, str>>,
    /// One-based source line, if supplied by the caller.
    pub line: Option<u32>,
    /// Wall-clock time on the calling thread, if enabled by [`Builder::with_time`].
    pub timestamp: Option<SystemTime>,
}

impl Record {
    fn from_log(record: &log::Record<'_>, timestamp: Option<SystemTime>) -> Self {
        let target = record
            .module_path_static()
            .filter(|path| *path == record.target())
            .map_or_else(|| Cow::Owned(record.target().to_owned()), Cow::Borrowed);
        let message = match record.args().as_str() {
            Some(message) => Cow::Borrowed(message),
            None => {
                let mut message = String::new();
                if fmt::write(&mut message, *record.args()).is_err() {
                    message.push_str("<formatting error>");
                }
                Cow::Owned(message)
            }
        };
        let module_path = record
            .module_path_static()
            .map(Cow::Borrowed)
            .or_else(|| record.module_path().map(|path| Cow::Owned(path.to_owned())));
        let file = record
            .file_static()
            .map(Cow::Borrowed)
            .or_else(|| record.file().map(|path| Cow::Owned(path.to_owned())));

        Self {
            level: record.level(),
            target,
            message,
            module_path,
            file,
            line: record.line(),
            timestamp,
        }
    }
}

/// Writes records on the logging worker thread.
///
/// Pass a concrete appender or a `Box<dyn Appender>` to [`Builder::init`].
///
/// Appenders must not log through the facade: enqueueing can block when the
/// queue is full, and the worker is the only consumer.
///
/// # Examples
///
/// ```
/// use std::io::{self, Write};
/// use sawmill::{Appender, Builder, LevelFilter, Record};
///
/// struct TextAppender<W>(W);
///
/// impl<W: Write + Send + 'static> Appender for TextAppender<W> {
///     fn append(&mut self, record: &Record) -> io::Result<()> {
///         writeln!(self.0, "{}: {}", record.level, record.message)
///     }
///
///     fn flush(&mut self) -> io::Result<()> {
///         self.0.flush()
///     }
/// }
///
/// # fn main() -> io::Result<()> {
/// let _guard = Builder::new(LevelFilter::Info).init(TextAppender(io::stdout()))?;
/// log::info!("Application started");
/// # Ok(())
/// # }
/// ```
pub trait Appender: Send + 'static {
    /// Writes a record to the appender's destination.
    ///
    /// # Errors
    ///
    /// Return an error if the record cannot be written. The worker keeps
    /// processing records and reports the first error through [`Guard::shutdown`].
    fn append(&mut self, record: &Record) -> io::Result<()>;

    /// Flushes buffered output. The default does nothing.
    ///
    /// # Errors
    ///
    /// Return an error if buffered output cannot be flushed.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<A: Appender + ?Sized> Appender for Box<A> {
    fn append(&mut self, record: &Record) -> io::Result<()> {
        (**self).append(record)
    }

    fn flush(&mut self) -> io::Result<()> {
        (**self).flush()
    }
}

/// Writes records to standard error as `LEVEL target: message`.
///
/// See [`Builder`] for an example.
#[derive(Clone, Copy, Debug, Default)]
pub struct StderrAppender;

impl Appender for StderrAppender {
    fn append(&mut self, record: &Record) -> io::Result<()> {
        writeln!(
            io::stderr().lock(),
            "{} {}: {}",
            record.level,
            record.target,
            record.message
        )
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stderr().lock().flush()
    }
}

/// Configures the process-wide logger before initialization.
///
/// # Examples
///
/// ```
/// use sawmill::{Builder, Level, LevelFilter, StderrAppender};
///
/// # fn main() -> std::io::Result<()> {
/// let guard = Builder::new(LevelFilter::Info)
///     .with_queue_capacity(256)
///     .with_blocking_level(Level::Info)
///     .with_thread_name("logger")
///     .with_time()
///     .init(StderrAppender)?;
/// log::info!("Application started");
/// sawmill::set_max_level(LevelFilter::Warn);
/// guard.shutdown()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Builder {
    max_level: LevelFilter,
    blocking_level: Level,
    capture_time: bool,
    capture_panics: bool,
    queue_capacity: usize,
    thread_name: Option<String>,
}

impl Builder {
    /// Sets the initial log level with timestamp capture disabled.
    pub fn new(max_level: LevelFilter) -> Self {
        Self {
            max_level,
            blocking_level: Level::Warn,
            capture_time: false,
            capture_panics: false,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            thread_name: None,
        }
    }

    /// Captures a wall-clock timestamp on the calling thread for each accepted record.
    #[must_use]
    pub fn with_time(mut self) -> Self {
        self.capture_time = true;
        self
    }

    /// Installs a process-wide hook that attempts to log panics at [`Level::Error`]
    /// with the `panic` target.
    ///
    /// The logging attempt respects the log level and does not wait for queue space.
    /// Panics on the logging worker are not enqueued. The previous hook is always
    /// called, even after shutdown. Queued records may not be written if the
    /// process aborts. Initialization with this option is not allowed during unwinding.
    #[must_use]
    pub fn with_panic_hook(mut self) -> Self {
        self.capture_panics = true;
        self
    }

    /// Sets the number of entries the queue can hold. The default is 1024.
    /// With zero capacity, blocking sends wait for the worker to receive the
    /// entry and nonblocking sends succeed only when the worker is ready.
    /// See [`Self::with_blocking_level`].
    #[must_use]
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Sets the least severe level that waits for queue space. The default is [`Level::Warn`].
    /// Less severe records are dropped if they cannot be queued immediately.
    /// Use [`Level::Trace`] to make every level wait for space. Flush still waits
    /// for the worker, and the optional panic hook never waits for queue space.
    #[must_use]
    pub fn with_blocking_level(mut self, level: Level) -> Self {
        self.blocking_level = level;
        self
    }

    /// Sets the worker thread name. The default is `log-appender`.
    /// Names containing a NUL byte are rejected by [`Self::init`].
    #[must_use]
    pub fn with_thread_name(mut self, name: impl Into<String>) -> Self {
        self.thread_name = Some(name.into());
        self
    }

    /// Installs the process-wide logger and starts its worker.
    ///
    /// Keep the returned [`Guard`] alive while logging. The `log` facade only
    /// allows one logger registration per process, even after shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the thread name contains a NUL byte, the worker
    /// cannot start, a logger is already installed, or panic hook installation
    /// is requested during unwinding.
    ///
    /// # Panics
    ///
    /// Panics if the queue capacity exceeds the standard library channel's
    /// supported range.
    pub fn init(self, appender: impl Appender) -> io::Result<Guard> {
        let thread_name = self.thread_name.unwrap_or_else(|| "log-appender".into());
        if thread_name.contains('\0') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker thread name cannot contain NUL bytes",
            ));
        }
        if self.capture_panics && thread::panicking() {
            return Err(io::Error::other(
                "cannot install a panic hook during unwinding",
            ));
        }

        let (sender, receiver) = mpsc::sync_channel(self.queue_capacity);
        let counters = Arc::new(Counters::default());
        let queue = Arc::new(Queue {
            closed: AtomicBool::new(false),
            sender: RwLock::new(Some(sender)),
            counters: Arc::clone(&counters),
        });
        let logger = Logger {
            queue: Arc::clone(&queue),
            blocking_level: self.blocking_level,
            capture_time: self.capture_time,
        };
        let worker = thread::Builder::new()
            .name(thread_name)
            .spawn(move || run(appender, receiver, &counters))?;
        let worker_id = worker.thread().id();
        let guard = Guard {
            queue,
            worker: Some(worker),
        };

        log::set_boxed_logger(Box::new(logger)).map_err(io::Error::other)?;
        set_max_level(self.max_level);
        if self.capture_panics {
            let queue = Arc::clone(&guard.queue);
            let capture_time = self.capture_time;
            let previous = panic::take_hook();
            panic::set_hook(Box::new(move |info| {
                if thread::current().id() != worker_id {
                    queue.try_log_panic(info, capture_time);
                }
                previous(info);
            }));
        }
        Ok(guard)
    }
}

/// Changes the process-wide runtime log filter.
///
/// Records already queued are still written. Levels disabled at compile time
/// cannot be enabled at runtime. See [`Builder`] for an example.
pub fn set_max_level(max_level: LevelFilter) {
    log::set_max_level(max_level);
}

/// A cloneable handle to the logger's cumulative counters.
///
/// Obtain it with [`Guard::metrics`]. Reading these counters does not block the
/// worker. Counts wrap on overflow.
#[derive(Clone, Debug)]
pub struct Metrics {
    counters: Arc<Counters>,
}

impl Metrics {
    /// Returns the number of records rejected because the queue was full.
    /// Includes panic-hook records, but excludes filtering and shutdown.
    /// With a zero-capacity queue, this counts attempts when the worker was not ready.
    pub fn queue_full_drops(&self) -> usize {
        self.counters.queue_full_drops.load(Ordering::Relaxed)
    }

    /// Returns the number of errors from [`Appender::append`].
    /// Flush errors are not counted.
    pub fn append_errors(&self) -> usize {
        self.counters.append_errors.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
struct Counters {
    queue_full_drops: AtomicUsize,
    append_errors: AtomicUsize,
}

/// Drains the queue, flushes the appender, and joins the worker on drop.
///
/// Dropping the guard blocks until the worker exits and discards any errors.
/// Use [`Guard::shutdown`] to observe those errors. Keep the guard outside the
/// appender: it must not be dropped on its own worker thread.
#[must_use = "dropping the guard shuts down the logger"]
pub struct Guard {
    queue: Arc<Queue>,
    worker: Option<JoinHandle<io::Result<()>>>,
}

impl fmt::Debug for Guard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Guard")
            .field("worker", &self.worker.as_ref().map(JoinHandle::thread))
            .finish_non_exhaustive()
    }
}

impl Guard {
    /// Returns a metrics handle that can be shared or retained after shutdown.
    pub fn metrics(&self) -> Metrics {
        Metrics {
            counters: Arc::clone(&self.queue.counters),
        }
    }

    /// Drains the queue, flushes the appender, and waits for the worker to exit.
    ///
    /// # Errors
    ///
    /// Returns the first appender error, or an error if the worker panicked.
    ///
    /// # Panics
    ///
    /// Calling this from the worker thread can panic or deadlock.
    pub fn shutdown(mut self) -> io::Result<()> {
        self.finish()
    }

    fn finish(&mut self) -> io::Result<()> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };

        self.queue.closed.store(true, Ordering::Relaxed);
        self.queue.sender.write().unwrap().take();
        worker
            .join()
            .map_err(|_| io::Error::other("logging worker panicked"))?
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

enum Command {
    Append(Record),
    Flush(SyncSender<()>),
}

struct Queue {
    closed: AtomicBool,
    sender: RwLock<Option<SyncSender<Command>>>,
    counters: Arc<Counters>,
}

impl Queue {
    fn try_append(&self, sender: &SyncSender<Command>, record: Record) {
        if matches!(
            sender.try_send(Command::Append(record)),
            Err(TrySendError::Full(_))
        ) {
            self.counters
                .queue_full_drops
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn try_log_panic(&self, info: &PanicHookInfo<'_>, capture_time: bool) {
        if Level::Error > log::STATIC_MAX_LEVEL
            || Level::Error > log::max_level()
            || self.closed.load(Ordering::Relaxed)
        {
            return;
        }

        let Ok(sender) = self.sender.try_read() else {
            return;
        };
        let Some(sender) = sender.as_ref() else {
            return;
        };

        let payload = info.payload();
        let message = if let Some(message) = payload.downcast_ref::<&'static str>() {
            Cow::Borrowed(*message)
        } else if let Some(message) = payload.downcast_ref::<String>() {
            Cow::Owned(message.clone())
        } else {
            Cow::Borrowed("panic with a non-string payload")
        };
        let location = info.location();
        let record = Record {
            level: Level::Error,
            target: Cow::Borrowed("panic"),
            message,
            module_path: None,
            file: location.map(|location| Cow::Owned(location.file().to_owned())),
            line: location.map(|location| location.line()),
            timestamp: capture_time.then(SystemTime::now),
        };
        self.try_append(sender, record);
    }
}

struct Logger {
    queue: Arc<Queue>,
    blocking_level: Level,
    capture_time: bool,
}

impl Logger {
    fn send(&self, command: Command) -> bool {
        self.queue
            .sender
            .read()
            .unwrap()
            .as_ref()
            .is_some_and(|sender| sender.send(command).is_ok())
    }
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::max_level() && !self.queue.closed.load(Ordering::Relaxed)
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let blocking = record.level() <= self.blocking_level;
        let timestamp = self.capture_time.then(SystemTime::now);
        let record = Record::from_log(record, timestamp);
        if blocking {
            self.send(Command::Append(record));
        } else if let Ok(sender) = self.queue.sender.try_read()
            && let Some(sender) = sender.as_ref()
        {
            self.queue.try_append(sender, record);
        }
    }

    fn flush(&self) {
        let (done, flushed) = mpsc::sync_channel(0);
        if self.send(Command::Flush(done)) {
            let _ = flushed.recv();
        }
    }
}

fn run(
    mut appender: impl Appender,
    receiver: Receiver<Command>,
    counters: &Counters,
) -> io::Result<()> {
    let mut first_error = None;
    for command in receiver {
        let result = match command {
            Command::Append(record) => {
                let result = appender.append(&record);
                if result.is_err() {
                    counters.append_errors.fetch_add(1, Ordering::Relaxed);
                }
                result
            }
            Command::Flush(done) => {
                let result = appender.flush();
                let _ = done.send(());
                result
            }
        };

        first_error = first_error.or_else(|| result.err());
    }

    let result = appender.flush();
    first_error.map_or(result, Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn record_preserves_source_location() {
        let owned = {
            let module_path = String::from("dynamic::module");
            let file = String::from("dynamic.rs");
            Record::from_log(
                &log::Record::builder()
                    .module_path(Some(&module_path))
                    .file(Some(&file))
                    .line(Some(42))
                    .build(),
                None,
            )
        };
        assert!(matches!(owned.module_path, Some(Cow::Owned(path)) if path == "dynamic::module"));
        assert!(matches!(owned.file, Some(Cow::Owned(path)) if path == "dynamic.rs"));
        assert_eq!(owned.line, Some(42));

        let borrowed = Record::from_log(
            &log::Record::builder()
                .module_path_static(Some("static::module"))
                .file_static(Some("static.rs"))
                .build(),
            None,
        );
        assert!(matches!(
            borrowed.module_path,
            Some(Cow::Borrowed("static::module"))
        ));
        assert!(matches!(borrowed.file, Some(Cow::Borrowed("static.rs"))));
        assert_eq!(borrowed.line, None);

        let missing = Record::from_log(&log::Record::builder().build(), None);
        assert!(missing.module_path.is_none());
        assert!(missing.file.is_none());
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Record(Level, String, String),
        Flush,
    }

    struct RecordingAppender {
        events: Arc<Mutex<Vec<Event>>>,
        expect_timestamp: bool,
        expected_thread_name: Option<&'static str>,
    }

    impl Appender for RecordingAppender {
        fn append(&mut self, record: &Record) -> io::Result<()> {
            assert_eq!(record.timestamp.is_some(), self.expect_timestamp);
            if let Some(name) = self.expected_thread_name {
                assert_eq!(thread::current().name(), Some(name));
            }
            self.events.lock().unwrap().push(Event::Record(
                record.level,
                record.target.to_string(),
                record.message.to_string(),
            ));
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.events.lock().unwrap().push(Event::Flush);
            Ok(())
        }
    }

    #[test]
    fn formatted_message_is_owned() {
        let record = {
            let value = String::from("first");
            Record::from_log(
                &log::Record::builder()
                    .args(format_args!("value={value}"))
                    .build(),
                None,
            )
        };
        assert!(matches!(record.message, Cow::Owned(message) if message == "value=first"));
    }

    #[test]
    fn literal_message_is_borrowed() {
        let record = Record::from_log(
            &log::Record::builder().args(format_args!("literal")).build(),
            None,
        );
        assert!(matches!(record.message, Cow::Borrowed("literal")));
    }

    #[test]
    fn formatting_error_keeps_partial_message() {
        struct Failing;
        impl fmt::Display for Failing {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                Err(fmt::Error)
            }
        }

        let record = Record::from_log(
            &log::Record::builder()
                .args(format_args!("value={Failing}"))
                .build(),
            None,
        );
        assert_eq!(record.message, "value=<formatting error>");
    }

    #[test]
    fn rejects_invalid_thread_name() {
        let result = Builder::new(LevelFilter::Info)
            .with_thread_name("invalid\0name")
            .init(StderrAppender);
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    }

    #[test]
    fn facade_flushes_and_guard_drains_on_drop() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let appender: Box<dyn Appender> = Box::new(RecordingAppender {
            events: Arc::clone(&events),
            expect_timestamp: true,
            expected_thread_name: Some("log-worker"),
        });
        let guard = Builder::new(LevelFilter::Info)
            .with_time()
            .with_queue_capacity(0)
            .with_blocking_level(Level::Trace)
            .with_thread_name("log-worker")
            .init(appender)
            .unwrap();

        let target = String::from("test-target");
        let value = String::from("owned message");
        assert!(log::log_enabled!(Level::Info));
        log::info!(target: &target, "{value}");
        log::debug!("filtered");
        drop((target, value));
        log::logger().flush();

        assert_eq!(
            *events.lock().unwrap(),
            [
                Event::Record(Level::Info, "test-target".into(), "owned message".into()),
                Event::Flush,
            ],
        );

        set_max_level(LevelFilter::Debug);
        log::debug!("enabled at runtime");
        set_max_level(LevelFilter::Warn);
        log::info!("disabled at runtime");
        log::warn!("pending at shutdown");
        drop(guard);
        assert!(!log::log_enabled!(Level::Error));

        struct TooLate;
        impl fmt::Display for TooLate {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("a record logged after shutdown should not be formatted")
            }
        }
        log::error!("{TooLate}");
        log::logger().flush();

        assert_eq!(
            events.lock().unwrap()[2..],
            [
                Event::Record(
                    Level::Debug,
                    module_path!().into(),
                    "enabled at runtime".into()
                ),
                Event::Record(
                    Level::Warn,
                    module_path!().into(),
                    "pending at shutdown".into()
                ),
                Event::Flush,
            ],
        );
    }
}
