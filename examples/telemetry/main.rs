//! Records raw request latencies in per-thread buffers and periodically
//! harvests them.

use std::{
    thread,
    time::{Duration, Instant},
};

use self::storage::Telemetry;

mod storage;

#[cfg(test)]
mod tests;

fn main() {
    const RUN_DURATION: Duration = Duration::from_secs(10);
    const HARVEST_INTERVAL: Duration = Duration::from_millis(1_100);

    let telemetry = Telemetry::default();
    let started = Instant::now();
    let deadline = started + RUN_DURATION;
    thread::scope(|scope| {
        let workers: Vec<_> = (1..=4)
            .map(|worker| {
                let telemetry = &telemetry;
                scope.spawn(move || {
                    while Instant::now() < deadline {
                        let started = Instant::now();
                        // Simulate handling a request.
                        thread::sleep(Duration::from_micros(worker * 100));
                        telemetry.record(u64::try_from(started.elapsed().as_micros()).unwrap());
                    }
                })
            })
            .collect();

        let mut next_report = started + HARVEST_INTERVAL;
        while next_report < deadline {
            thread::sleep(next_report.saturating_duration_since(Instant::now()));
            report(&telemetry);
            next_report += HARVEST_INTERVAL;
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });

    // ThreadLocal keeps handles alive after their writer threads exit.
    report(&telemetry);
}

fn report(telemetry: &Telemetry) {
    let samples = telemetry.harvest();
    let mut count = 0;
    let mut sum = 0_u64;
    let mut max = 0;
    for latency_us in samples {
        count += 1;
        sum += latency_us;
        max = max.max(latency_us);
    }
    if let Some(mean) = sum.checked_div(count) {
        println!("request latency: count={count}, mean={mean}us, max={max}us");
    }
}
