use std::env;
use std::hint::{black_box, spin_loop};
use std::io;
use std::process::Command;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use sawmill::{Appender, Builder, Level, LevelFilter, Record};

const PRODUCERS: [usize; 3] = [1, 4, 8];
const WORKLOADS: [(&str, usize); 3] = [
    ("literal", 0),
    ("formatted-64", 64),
    ("formatted-2048", 2048),
];

static APPENDED: AtomicUsize = AtomicUsize::new(0);

struct CountAppender {
    count: usize,
    delay: Duration,
}

impl Appender for CountAppender {
    fn append(&mut self, record: &Record) -> io::Result<()> {
        black_box(record.message.as_bytes());
        if !self.delay.is_zero() {
            let started = Instant::now();
            while started.elapsed() < self.delay {
                spin_loop();
            }
        }
        self.count += 1;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        APPENDED.store(self.count, Ordering::Relaxed);
        Ok(())
    }
}

fn emit(records: usize, payload: &str, mut latencies: Option<&mut Vec<u64>>) {
    for sequence in 0..records {
        let started = (latencies.is_some() && sequence.is_multiple_of(67)).then(Instant::now);
        if payload.is_empty() {
            log::info!("a fixed message with no formatting");
        } else {
            log::info!("message {sequence}: {payload}");
        }
        if let Some(started) = started {
            latencies
                .as_mut()
                .unwrap()
                .push(started.elapsed().as_nanos() as u64);
        }
    }
}

fn sample(
    producers: usize,
    payload_len: usize,
    records: usize,
    queue_capacity: usize,
    appender_delay: Duration,
) -> (Duration, Duration, u64) {
    let guard = Builder::new(LevelFilter::Info)
        .with_queue_capacity(queue_capacity)
        .with_blocking_level(Level::Trace)
        .init(CountAppender {
            count: 0,
            delay: appender_delay,
        })
        .unwrap();
    let payload = "x".repeat(payload_len);
    let ready = Barrier::new(producers + 1);
    let start = Barrier::new(producers + 1);
    let submitted = Barrier::new(producers + 1);
    let per_producer = records / producers;
    let warmup = per_producer.min(5_000);

    let stats = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(producers);
        for _ in 0..producers {
            handles.push(scope.spawn(|| {
                let mut latencies = Vec::with_capacity(per_producer.div_ceil(67));
                emit(warmup, &payload, None);
                ready.wait();
                start.wait();
                emit(per_producer, &payload, Some(&mut latencies));
                submitted.wait();
                latencies
            }));
        }

        ready.wait();
        log::logger().flush();
        assert_eq!(APPENDED.load(Ordering::Relaxed), producers * warmup);

        let started = Instant::now();
        start.wait();
        submitted.wait();
        let enqueue = started.elapsed();
        log::logger().flush();
        let drained = started.elapsed();
        assert_eq!(
            APPENDED.load(Ordering::Relaxed),
            producers * warmup + records
        );
        let mut latencies: Vec<_> = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect();
        latencies.sort_unstable();
        let p99 = latencies[latencies.len() - latencies.len() / 100 - 1];
        (enqueue, drained, p99)
    });

    guard.shutdown().unwrap();
    stats
}

fn setting(name: &str, default: usize) -> usize {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("invalid {name}: {value}")),
        Err(env::VarError::NotPresent) => default,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn format_samples(times: &mut [f64]) -> String {
    times.sort_by(f64::total_cmp);
    let median = (times[(times.len() - 1) / 2] + times[times.len() / 2]) / 2.0;
    format!(
        "{median:5.2} [{:5.2}, {:5.2}]",
        times[0],
        times[times.len() - 1]
    )
}

fn main() {
    let args: Vec<_> = env::args().skip(1).collect();
    let records = setting("SAWMILL_BENCH_RECORDS", 200_000);
    let queue_capacity = setting("SAWMILL_BENCH_QUEUE_CAPACITY", 1024);
    let appender_delay = setting("SAWMILL_BENCH_APPENDER_NS", 0);
    match args.as_slice() {
        [mode, producers, payload_len] if mode == "--sample" => {
            let (enqueue, drained, p99) = sample(
                producers.parse().unwrap(),
                payload_len.parse().unwrap(),
                records,
                queue_capacity,
                Duration::from_nanos(appender_delay as u64),
            );
            println!("{} {} {p99}", enqueue.as_nanos(), drained.as_nanos());
            return;
        }
        [mode] if mode == "--bench" => {}
        _ => return,
    }

    let repeats = setting("SAWMILL_BENCH_REPEATS", 3);
    assert!(repeats > 0, "SAWMILL_BENCH_REPEATS must be positive");
    assert!(
        records > 0 && records.is_multiple_of(8),
        "SAWMILL_BENCH_RECORDS must be a positive multiple of 8"
    );
    let executable = env::current_exe().unwrap();
    println!("{records} records/sample, {repeats} samples/case, queue capacity {queue_capacity}");
    println!(
        "Median [min, max]; throughput in million records/sec; appender busy-waits {appender_delay} ns/record"
    );
    println!(
        "{:>7}  {:<14}  {:>22}  {:>22}  {:>22}",
        "Threads", "Workload", "Submission", "Through drain", "Submit p99 (us)"
    );
    for producers in PRODUCERS {
        for (workload, payload_len) in WORKLOADS {
            let mut submit_rates = Vec::with_capacity(repeats);
            let mut drain_rates = Vec::with_capacity(repeats);
            let mut p99_latencies = Vec::with_capacity(repeats);
            for _ in 0..repeats {
                let output = Command::new(&executable)
                    .args(["--sample", &producers.to_string(), &payload_len.to_string()])
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "sample failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let output = std::str::from_utf8(&output.stdout).unwrap();
                let mut values = output
                    .split_whitespace()
                    .map(|value| value.parse::<f64>().unwrap());
                let rate = |nanos: f64| records as f64 * 1_000.0 / nanos;
                submit_rates.push(rate(values.next().unwrap()));
                drain_rates.push(rate(values.next().unwrap()));
                p99_latencies.push(values.next().unwrap() / 1_000.0);
            }
            println!(
                "{producers:>7}  {workload:<14}  {:>22}  {:>22}  {:>22}",
                format_samples(&mut submit_rates),
                format_samples(&mut drain_rates),
                format_samples(&mut p99_latencies)
            );
        }
    }
}
