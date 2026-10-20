use crate::{
    barrier,
    sync::{AtomicU8, Ordering, UnsafeCell},
};

pub(crate) struct RawCell<T> {
    state: State,
    buffers: [UnsafeCell<T>; 2],
}

// 4 divides the cache-line size, so this 4-byte aligned struct stays within one
// cache line.
#[repr(align(4))]
struct State {
    /// Epoch claimed by the writer, or [`IDLE`] between writes.
    writer_claim: AtomicU8,
    /// Selects buffer 0 or 1 for new writes. Only the harvester advances it.
    write_epoch: AtomicU8,
    /// Epoch of the pending harvest, or [`IDLE`]. Only the harvester accesses
    /// it. Stored in the cell so interrupted harvesting does not lose the
    /// epoch.
    pending_harvest: AtomicU8,
}

const IDLE: u8 = u8::MAX;

const USE_SC_ATOMICS: bool = !cfg!(feature = "fence_fallback");
const CLAIM_ORDER: Ordering = if USE_SC_ATOMICS {
    Ordering::SeqCst
} else {
    Ordering::Release
};
const CONFIRM_ORDER: Ordering = if USE_SC_ATOMICS {
    Ordering::SeqCst
} else {
    Ordering::Acquire
};

// SAFETY: the epoch protocol excludes concurrent buffer access under the
// single-writer, single-harvester contract.
unsafe impl<T: Send> Sync for RawCell<T> {}

impl<T> RawCell<T> {
    /// Creates a cell containing the two buffers.
    pub(crate) fn new(buffers: [T; 2]) -> Self {
        Self {
            state: State {
                writer_claim: AtomicU8::new(IDLE),
                write_epoch: AtomicU8::new(0),
                pending_harvest: AtomicU8::new(IDLE),
            },
            buffers: buffers.map(UnsafeCell::new),
        }
    }

    /// Calls the callback with the current write buffer.
    ///
    /// # Panics
    ///
    /// A panicking callback releases [`State::writer_claim`] without rolling
    /// back buffer changes.
    ///
    /// # Safety
    ///
    /// The caller must be the single writer and exclude nested writes.
    #[inline]
    pub(crate) unsafe fn write<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let barrier: barrier::Barrier = barrier::get();
        let _guard = IdleGuard(&self.state.writer_claim);

        let mut epoch = self.state.write_epoch.load(Ordering::Relaxed);
        loop {
            self.state.writer_claim.store(epoch, CLAIM_ORDER);
            if !USE_SC_ATOMICS {
                barrier.light();
            }
            let confirmed = self.state.write_epoch.load(CONFIRM_ORDER);
            if confirmed == epoch {
                break;
            }
            epoch = confirmed;
        }

        // SAFETY: epochs are 0/1; the confirmed claim and single-writer
        // contract exclude conflicting access.
        unsafe {
            self.buffers
                .get_unchecked(usize::from(epoch))
                .with_mut(|ptr| f(&mut *ptr))
        }
    }

    /// Switches writes to the other buffer and marks the write buffer as
    /// pending for harvest, unless a harvest is already pending.
    ///
    /// # Safety
    ///
    /// The caller must be the single harvester.
    pub(crate) unsafe fn advance(&self) {
        if self.state.pending_harvest.load(Ordering::Relaxed) != IDLE {
            return;
        }
        let epoch = self.state.write_epoch.load(Ordering::Relaxed);
        self.state.pending_harvest.store(epoch, Ordering::Relaxed);
        // Publish changes from the previous harvest before the writer reuses
        // its buffer.
        self.state.write_epoch.store(epoch ^ 1, Ordering::Release);
    }

    /// Checks whether the writer has left the pending buffer.
    ///
    /// Call after [`advance`](Self::advance) and
    /// [`Barrier::heavy()`](barrier::Barrier::heavy). One barrier call can
    /// cover multiple advanced cells.
    #[inline]
    pub(crate) fn can_harvest(&self) -> bool {
        let pending = self.state.pending_harvest.load(Ordering::Relaxed);
        debug_assert_ne!(pending, IDLE);
        self.state.writer_claim.load(Ordering::Acquire) != pending
    }

    /// Calls the callback with the pending buffer and completes its harvest.
    ///
    /// # Panics
    ///
    /// A panicking callback leaves the harvest pending without rolling back
    /// buffer changes.
    ///
    /// # Safety
    ///
    /// The cell must have a pending harvest.
    /// The caller must be the single harvester, call
    /// [`Barrier::heavy()`](barrier::Barrier::heavy), and observe
    /// [`can_harvest`](Self::can_harvest) returning `true` after
    /// [`advance`](Self::advance). No other harvester operation may run
    /// during this call.
    pub(crate) unsafe fn harvest<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let pending = self.state.pending_harvest.load(Ordering::Relaxed);
        debug_assert_ne!(pending, IDLE);
        // SAFETY: pending is 0/1, and readiness after the barrier excludes the
        // writer.
        let result = unsafe {
            self.buffers
                .get_unchecked(usize::from(pending))
                .with_mut(|ptr| f(&mut *ptr))
        };
        self.state.pending_harvest.store(IDLE, Ordering::Relaxed);
        result
    }
}

struct IdleGuard<'a>(&'a AtomicU8);

impl Drop for IdleGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.store(IDLE, Ordering::Release);
    }
}

#[cfg(all(test, not(all(harvest_cell_loom, feature = "loom"))))]
mod tests;
