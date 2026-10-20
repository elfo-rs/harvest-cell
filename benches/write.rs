//! Measures one counter update without concurrent harvesting.
//!
//! Run `cargo bench --bench write` for the default strategy. It uses `SeqCst`
//! atomics on all architectures. Add `--features fence_fallback` to measure an
//! explicit hardware fence. On Linux, run `cargo bench --bench write --features
//! membarrier` for membarrier.

use std::{
    hint::black_box,
    sync::{Arc, Mutex},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use harvest_cell::HarvestCell;

fn write(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Elements(1));

    group.bench_function("std_mutex", |b| {
        let value = black_box(Arc::new(Mutex::new(0_u64)));
        b.iter(|| update(&mut value.lock().unwrap()));
        black_box(*value.lock().unwrap());
    });

    group.bench_function("parking_lot", |b| {
        let value = black_box(Arc::new(parking_lot::Mutex::new(0_u64)));
        b.iter(|| update(&mut value.lock()));
        black_box(*value.lock());
    });

    #[cfg(all(feature = "membarrier", target_os = "linux", not(miri)))]
    if !membarrier_available() {
        group.finish();
        return;
    }

    let name = if cfg!(all(feature = "membarrier", target_os = "linux", not(miri))) {
        "harvest_cell/membarrier"
    } else if cfg!(feature = "fence_fallback") {
        "harvest_cell/fence"
    } else {
        "harvest_cell/seqcst"
    };
    group.bench_function(name, |b| {
        let (mut writer, _handle) = black_box(HarvestCell::new([0_u64; 2]).into_split());
        b.iter(|| writer.write(update));
        black_box(writer.write(|value| *value));
    });

    group.finish();
}

#[inline]
fn update(value: &mut u64) {
    *value = value.wrapping_add(1);
}

#[cfg(all(feature = "membarrier", target_os = "linux", not(miri)))]
fn membarrier_available() -> bool {
    // Skip unsupported systems instead of reporting the fallback as membarrier.
    // SAFETY: The arguments match the membarrier ABI and contain no pointers.
    let result = unsafe {
        libc::syscall(
            libc::SYS_membarrier,
            libc::MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED,
            0 as libc::c_uint,
            0 as libc::c_int,
        )
    };
    if result != 0 {
        eprintln!("Skipping membarrier: {}", std::io::Error::last_os_error());
        return false;
    }
    true
}

criterion_group!(benches, write);
criterion_main!(benches);
