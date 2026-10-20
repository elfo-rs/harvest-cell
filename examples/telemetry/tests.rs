use std::thread;

use super::storage::Telemetry;

#[test]
fn telemetry_harvests_each_sample_once_in_writer_order() {
    const THREADS: usize = 4;
    const SAMPLES: u64 = if cfg!(miri) { 16 } else { 1_000 };

    let telemetry = Telemetry::default();
    let mut values = vec![Vec::new(); THREADS];
    assert_eq!(telemetry.harvest().as_slice(), []);

    thread::scope(|scope| {
        let mut workers = Vec::with_capacity(THREADS);
        for worker in 0..THREADS {
            let telemetry = &telemetry;
            workers.push(scope.spawn(move || {
                let base = u64::try_from(worker).unwrap() * SAMPLES;
                for sample in 0..SAMPLES {
                    telemetry.record_with(|| {
                        let started = std::time::Instant::now();
                        while started.elapsed() < std::time::Duration::from_micros(10) {
                            std::hint::spin_loop();
                        }
                        base + sample
                    });
                    thread::yield_now();
                }
            }));
        }

        while workers.iter().any(|worker| !worker.is_finished()) {
            append_samples(&telemetry, &mut values, SAMPLES);
            thread::yield_now();
        }
    });

    append_samples(&telemetry, &mut values, SAMPLES);
    for values in values {
        assert_eq!(values, (0..SAMPLES).collect::<Vec<_>>());
    }
    assert_eq!(telemetry.harvest().as_slice(), []);
}

fn append_samples(telemetry: &Telemetry, values: &mut [Vec<u64>], samples_per_writer: u64) {
    for sample in telemetry.harvest() {
        let writer = usize::try_from(sample / samples_per_writer).unwrap();
        values[writer].push(sample % samples_per_writer);
    }
}
