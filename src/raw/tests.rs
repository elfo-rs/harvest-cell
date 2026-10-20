use std::{
    mem,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Barrier,
    thread,
};

use super::*;

#[test]
fn waits_for_the_writer_to_leave_the_pending_buffer() {
    let cell = RawCell::new([0, 0]);
    let entered = Barrier::new(2);
    let release = Barrier::new(2);
    thread::scope(|scope| {
        let writer = scope.spawn(|| {
            // SAFETY: this thread is the single writer without nested writes.
            unsafe {
                cell.write(|value| {
                    *value = 40;
                    entered.wait();
                    release.wait();
                    *value += 2;
                });
            }
        });
        entered.wait();
        // SAFETY: this thread is the single harvester.
        unsafe { cell.advance() };
        barrier::get().heavy();
        assert!(!cell.can_harvest());
        // SAFETY: the same harvester resumes its pending harvest.
        unsafe { cell.advance() };
        barrier::get().heavy();
        assert!(!cell.can_harvest());
        release.wait();
        writer.join().unwrap();
        // SAFETY: this thread is still the single harvester.
        assert_eq!(unsafe { take(&cell) }, 42);
    });
}

#[test]
fn writer_panic_releases_the_claim_and_keeps_changes() {
    let cell = RawCell::new([0, 0]);
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: this test is the single writer and does not reenter write.
        unsafe {
            cell.write(|value| {
                *value = 40;
                panic!("writer callback failed");
            });
        }
    }));
    assert!(result.is_err());
    assert_eq!(cell.state.writer_claim.load(Ordering::Relaxed), IDLE);
    // SAFETY: this test owns both rights and serializes all accesses.
    unsafe {
        cell.write(|value| *value += 2);
        assert_eq!(take(&cell), 42);
    }
}

#[test]
fn harvester_panic_keeps_the_epoch_and_partial_changes() {
    let cell = RawCell::new([40, 7]);
    // SAFETY: this test is the single harvester.
    unsafe { cell.advance() };
    barrier::get().heavy();
    assert!(cell.can_harvest());
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the test holds the harvester right and observed readiness
        // after the barrier.
        unsafe {
            cell.harvest(|value| {
                *value += 2;
                panic!("harvester callback failed");
            });
        }
    }));
    assert!(result.is_err());
    assert_ne!(cell.state.pending_harvest.load(Ordering::Relaxed), IDLE);
    // SAFETY: this test owns both rights and serializes all accesses.
    unsafe {
        cell.write(|value| *value += 1);
        assert_eq!(take(&cell), 42);
        assert_eq!(take(&cell), 8);
    }
}

#[test]
fn writer_observes_the_replacement_from_a_previous_harvest() {
    let cell = RawCell::new([vec![1], Vec::new()]);
    // SAFETY: this test owns both rights and serializes all accesses.
    unsafe {
        cell.advance();
        barrier::get().heavy();
        assert!(cell.can_harvest());
        let values = cell.harvest(|values| mem::replace(values, Vec::with_capacity(8)));
        assert_eq!(values, [1]);
        assert_eq!(take(&cell).as_slice(), []);
        cell.write(|values| {
            assert_eq!(values.as_slice(), []);
            assert!(values.capacity() >= 8);
            values.push(2);
        });
        assert_eq!(take(&cell), [2]);
    }
}

#[test]
fn concurrent_writes_are_harvested_once() {
    const WRITES: u64 = if cfg!(miri) { 64 } else { 10_000 };
    let cell = RawCell::new([Vec::new(), Vec::new()]);
    let mut values = Vec::new();
    thread::scope(|scope| {
        let writer = scope.spawn(|| {
            for value in 0..WRITES {
                // SAFETY: this thread is the single writer without nested
                // writes.
                unsafe {
                    cell.write(|values| {
                        values.push(value);
                        for _ in 0..256 {
                            std::hint::spin_loop();
                        }
                    });
                }
            }
        });
        while !writer.is_finished() {
            // SAFETY: this thread is the single harvester and completes each
            // harvest.
            values.extend(unsafe { take(&cell) });
        }
        writer.join().unwrap();
        // SAFETY: this thread is the single harvester; flush writer buffer.
        unsafe {
            values.extend(take(&cell));
        }
    });
    assert_eq!(values, (0..WRITES).collect::<Vec<_>>());
}

// SAFETY: the caller must be the single harvester for this cell.
unsafe fn take<T: Default>(cell: &RawCell<T>) -> T {
    // SAFETY: the caller holds the harvester right.
    unsafe { cell.advance() };
    barrier::get().heavy();
    while !cell.can_harvest() {
        thread::yield_now();
    }
    // SAFETY: the barrier and readiness check cover the pending harvest.
    unsafe { cell.harvest(mem::take) }
}
