# Sawmill

Sawmill is a Rust backend for the `log` facade. It sends records through a
bounded queue to a worker thread, where an appender writes them. Its only
dependency is `log`.

## Quick start

To use a local checkout from another Cargo project, add these dependencies
and adjust the path as needed:

```toml
[dependencies]
sawmill = { path = "../sawmill" }
log = "0.4"
```

```rust
use sawmill::{Builder, LevelFilter, StderrAppender};

fn main() -> std::io::Result<()> {
    let _guard = Builder::new(LevelFilter::Info).init(StderrAppender)?;

    log::info!("Application started");

    Ok(())
}
```

Run the bundled example from the checkout with `cargo run --example stderr`.

The logger can be installed only once per process. Bind the guard to a name
like `_guard` to keep it alive until the scope ends; `let _ = ...` drops it
immediately. When the guard drops, it drains the queue, flushes the appender,
and joins the worker. Call `guard.shutdown()` explicitly to report an appender
error or worker panic instead of ignoring it during drop.

## Queue and filtering

The default queue holds 1024 entries. By default, `Error` and `Warn` records 
block the calling thread when it fills; less severe records are dropped if
they cannot be queued immediately. Use `with_queue_capacity` to resize the queue and
`with_blocking_level` to change which levels wait. Set the blocking level to
`Level::Trace` to make every level wait for space.

Use `guard.metrics()` to get a shareable handle for monitoring.
`queue_full_drops()` counts records rejected for lack of queue space;
`append_errors()` counts failed calls to `Appender::append`, not flush errors.
Both counts are cumulative and remain readable after shutdown.

Use `sawmill::set_max_level` to change the runtime filter. Already queued
records are unaffected, and levels disabled at compile time remain disabled.
`with_time()` captures wall-clock timestamps on the calling thread, and
`with_thread_name()` names the worker. The opt-in `with_panic_hook()` attempts
to enqueue panics without waiting for space and still calls the previous hook.

## Output

`StderrAppender` writes `LEVEL target: message` to standard error. Implement
`Appender` to choose another destination or format. Appenders receive owned
record data on the worker thread; message interpolation happens before queueing
and structured key-value fields are not captured. Do not log through the
facade from an appender: it can block waiting for the worker itself.
