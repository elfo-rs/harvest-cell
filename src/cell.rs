use std::{fmt, ptr::NonNull};

use sealed::sealed;

use crate::{raw::RawCell, sync::Arc};

/// Accumulates data for periodic harvesting without making the writer wait
/// for the harvester to finish processing a buffer.
///
/// Each cell has two buffers: the writer updates one while the harvester
/// collects the other. To start a harvest, the harvester requests buffer
/// switches for a group of cells and performs one memory barrier for the group.
/// It collects each cell's previous buffer once any write still using that
/// buffer finishes. Subsequent writes use the other buffer without waiting for
/// the harvest to finish. This is useful when writes are frequent and
/// harvesting is infrequent.
///
/// To coordinate these switches, the writer reads which buffer to use and marks
/// it as busy before each write.
/// It then checks whether the harvester has requested a buffer switch.
/// If so, it retries with the other buffer before calling the write callback.
/// By default, the store that marks the buffer as busy and the load that checks
/// for a switch use [`SeqCst`](std::sync::atomic::Ordering::SeqCst) ordering.
/// With `fence_fallback`, they use
/// [`Release`](std::sync::atomic::Ordering::Release)
/// and [`Acquire`](std::sync::atomic::Ordering::Acquire) ordering, with an
/// explicit [hardware fence](std::sync::atomic::fence) between them.
///
/// On Linux, the `membarrier` feature builds on `fence_fallback`: it keeps the
/// `Release` and `Acquire` orderings but replaces the writer's hardware fence
/// with a [compiler fence](std::sync::atomic::compiler_fence). The harvester
/// issues a [`membarrier`](https://man7.org/linux/man-pages/man2/membarrier.2.html)
/// syscall instead of a hardware fence. This combination of barriers gives
/// both sides the same ordering guarantees as `fence_fallback` but moves the
/// barrier cost from every write to the harvest: the syscall is paid once and
/// amortized across all cells in that harvest. Part of that cost falls on
/// other threads: the syscall may send inter-processor interrupts (IPIs) to
/// CPUs running other threads in the process, including writers.
///
/// If syscall registration fails, both sides use hardware fences as with
/// `fence_fallback`. The same applies on other operating systems, under Miri,
/// and under loom.
///
/// # Examples
///
/// ```
/// use std::{mem, thread};
/// use harvest_cell::{HarvestCell, Harvester};
///
/// const WRITES: usize = 100;
/// let mut cell = HarvestCell::new([Vec::new(), Vec::new()]);
/// let (mut writer, mut handle) = cell.split();
///
/// let values = thread::scope(|scope| {
///     scope.spawn(move || {
///         for value in 0..WRITES {
///             writer.write(|values| values.push(value));
///         }
///     });
///     let reader = scope.spawn(move || {
///         let mut values = Vec::new();
///         while values.len() < WRITES {
///             let mut harvester = Harvester::start(&mut handle);
///             while let Some(buffer) = harvester.next_with(mem::take) {
///                 values.extend(buffer);
///             }
///         }
///         values
///     });
///     reader.join().unwrap()
/// });
/// assert_eq!(values, (0..WRITES).collect::<Vec<_>>());
/// ```
pub struct HarvestCell<T: Send> {
    inner: RawCell<T>,
}

impl<T: Send> HarvestCell<T> {
    /// Creates a cell containing the two buffers.
    ///
    /// Writes start in `buffers[0]`. Starting the first harvest switches
    /// subsequent writes to `buffers[1]` so `buffers[0]` can be collected.
    pub fn new(buffers: [T; 2]) -> Self {
        Self {
            inner: RawCell::new(buffers),
        }
    }

    /// Borrows the cell as a writer and a harvest handle.
    ///
    /// See [`HarvestCell<T>`] for an example.
    pub fn split(&mut self) -> (Writer<Borrowed<'_, T>>, HarvestHandle<Borrowed<'_, T>>) {
        (
            Writer {
                storage: Borrowed {
                    buffer: &self.inner,
                },
            },
            HarvestHandle {
                storage: Borrowed {
                    buffer: &self.inner,
                },
            },
        )
    }

    /// Splits the cell into an owned writer and an owned harvest handle.
    ///
    /// This moves the buffers into a heap allocation shared by the writer and
    /// the harvest handle through an [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{mem, thread};
    /// use harvest_cell::{HarvestCell, Harvester};
    ///
    /// let (mut writer, mut handle) =
    ///     HarvestCell::new([Vec::new(), Vec::new()]).into_split();
    ///
    /// thread::spawn(move || {
    ///     writer.write(|values| values.push(42));
    /// });
    ///
    /// let res = thread::spawn(move || {
    ///     loop {
    ///         let mut harvester = Harvester::start(&mut handle);
    ///         match harvester.next_with(mem::take) {
    ///             Some(buffer) if !buffer.is_empty() => return buffer,
    ///             _ => thread::yield_now(),
    ///         }
    ///     }
    /// }).join().unwrap();
    ///
    /// assert_eq!(res, vec![42]);
    /// ```
    pub fn into_split(self) -> (Writer<Owned<T>>, HarvestHandle<Owned<T>>) {
        Self::map_split(self, |cell| cell)
    }

    /// Splits a cell inside custom storage into an owned writer and harvest
    /// handle.
    ///
    /// Moves `storage` into an [`Arc`] and calls `project` once to select the
    /// cell. Both the writer and the harvest handle keep `storage` alive until
    /// they are dropped.
    ///
    /// # Panics
    ///
    /// If `project` panics, the panic propagates and the storage is dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_utils::CachePadded;
    /// use harvest_cell::{HarvestCell, Harvester};
    ///
    /// let storage = CachePadded::new(HarvestCell::new([0, 0]));
    /// let (mut writer, mut handle) =
    ///     HarvestCell::map_split(storage, |padded| &mut **padded);
    ///
    /// writer.write(|value| *value = 42);
    /// let mut harvester = Harvester::start(&mut handle);
    /// assert_eq!(harvester.next_with(std::mem::take), Some(42));
    /// ```
    #[allow(clippy::type_complexity)] // Keep both return types explicit.
    pub fn map_split<S: Send>(
        storage: S,
        project: impl FnOnce(&mut S) -> &mut Self,
    ) -> (Writer<Owned<T, S>>, HarvestHandle<Owned<T, S>>) {
        let mut owner = Arc::new(storage);
        let cell = project(Arc::get_mut(&mut owner).unwrap());
        let buffer = NonNull::from(&cell.inner);
        (
            Writer {
                storage: Owned {
                    _owner: Arc::clone(&owner),
                    buffer,
                },
            },
            HarvestHandle {
                storage: Owned {
                    _owner: owner,
                    buffer,
                },
            },
        )
    }
}

/// Holds the exclusive right to write to a cell.
///
/// Created by [`HarvestCell::split()`], [`HarvestCell::into_split()`], or
/// [`HarvestCell::map_split()`].
pub struct Writer<S> {
    storage: S,
}

impl<S: Storage> Writer<S> {
    /// Calls the callback with the current write buffer and returns its result.
    ///
    /// See [`HarvestCell<T>`] for an example.
    ///
    /// # Panics
    ///
    /// If the callback panics, changes already made to the buffer remain.
    /// Writing and harvesting can continue after the panic is caught.
    #[inline]
    pub fn write<R>(&mut self, f: impl FnOnce(&mut S::Buffer) -> R) -> R {
        let storage = self.storage.as_borrowed();
        // SAFETY: this is a single writer and `&mut self` excludes nested
        // writes.
        unsafe { storage.buffer.write(f) }
    }
}

/// Holds the exclusive right to harvest a cell.
///
/// Created by [`HarvestCell::split()`], [`HarvestCell::into_split()`], or
/// [`HarvestCell::map_split()`].
/// Pass `&mut handle` to [`Harvester::start()`](crate::Harvester::start) to
/// start harvesting, alone or in an iterator with other handles.
pub struct HarvestHandle<S> {
    storage: S,
}

impl<S: Storage> HarvestHandle<S> {
    /// Returns a borrowed handle to the same cell.
    #[inline]
    #[must_use]
    pub fn as_borrowed(&mut self) -> HarvestHandle<Borrowed<'_, S::Buffer>> {
        HarvestHandle {
            storage: self.storage.as_borrowed(),
        }
    }
}

impl<'a, S: Storage> From<&'a mut HarvestHandle<S>> for HarvestHandle<Borrowed<'a, S::Buffer>> {
    #[inline]
    fn from(handle: &'a mut HarvestHandle<S>) -> Self {
        handle.as_borrowed()
    }
}

impl<T: Send> HarvestHandle<Borrowed<'_, T>> {
    /// Switches writes to the other buffer and marks the write buffer as
    /// pending for harvest, unless a harvest is already pending.
    pub(crate) fn advance(self) -> Self {
        // SAFETY: this handle holds the single harvester right.
        unsafe { self.storage.buffer.advance() };
        self
    }

    /// Checks readiness after [`advance`](Self::advance) and
    /// [`Barrier::heavy()`](crate::barrier::Barrier::heavy).
    #[inline]
    pub(crate) fn can_harvest(&self) -> bool {
        self.storage.buffer.can_harvest()
    }

    /// Calls the callback with the pending buffer and completes its harvest.
    ///
    /// # Safety
    ///
    /// The caller must call
    /// [`Barrier::heavy()`](crate::barrier::Barrier::heavy)
    /// after [`advance`](Self::advance) and observe
    /// [`can_harvest`](Self::can_harvest) returning `true` for the pending
    /// harvest.
    pub(crate) unsafe fn harvest<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: this handle holds the single harvester right; the caller
        // guarantees the barrier and readiness check. &mut self excludes
        // reentry.
        unsafe { self.storage.buffer.harvest(f) }
    }
}

/// Provides access to a cell's buffers for writers and harvest handles.
///
/// # Examples
///
/// ```
/// use harvest_cell::{HarvestCell, Storage, Writer};
///
/// fn increment<S: Storage<Buffer = u64>>(writer: &mut Writer<S>) -> u64 {
///     writer.write(|value| {
///         *value += 1;
///         *value
///     })
/// }
///
/// let mut cell = HarvestCell::new([0, 0]);
/// let (mut borrowed_writer, _) = cell.split();
/// let (mut owned_writer, _) = HarvestCell::new([0, 0]).into_split();
///
/// assert_eq!(increment(&mut borrowed_writer), 1);
/// assert_eq!(increment(&mut owned_writer), 1);
/// ```
#[sealed]
pub trait Storage {
    /// The buffer type stored in the cell.
    type Buffer: Send;

    /// Borrows the storage.
    #[must_use]
    fn as_borrowed(&self) -> Borrowed<'_, Self::Buffer>;
}

/// Storage that borrows a cell.
///
/// See [`HarvestCell::split()`] and [`HarvestHandle::as_borrowed()`].
pub struct Borrowed<'a, T: Send> {
    buffer: &'a RawCell<T>,
}

#[sealed]
impl<T: Send> Storage for Borrowed<'_, T> {
    type Buffer = T;

    #[inline]
    fn as_borrowed(&self) -> Borrowed<'_, T> {
        Borrowed {
            buffer: self.buffer,
        }
    }
}

/// Storage that shares a cell through an [`Arc`].
///
/// See [`HarvestCell::into_split()`] and [`HarvestCell::map_split()`].
pub struct Owned<T: Send, S: Send = HarvestCell<T>> {
    _owner: Arc<S>,
    buffer: NonNull<RawCell<T>>,
}

// SAFETY: T: Send makes RawCell<T> Send + Sync. After projection, the owner
// is only kept alive and dropped; neither S nor Arc<S> is exposed.
// S: Send allows to drop the owner on another thread.
unsafe impl<T: Send, S: Send> Send for Owned<T, S> {}
// SAFETY: Shared access exposes only RawCell<T>, which is Sync for T: Send.
// The owner is never accessed through shared references after projection.
unsafe impl<T: Send, S: Send> Sync for Owned<T, S> {}

#[sealed]
impl<T: Send, S: Send> Storage for Owned<T, S> {
    type Buffer = T;

    #[inline]
    fn as_borrowed(&self) -> Borrowed<'_, T> {
        Borrowed {
            // SAFETY: _owner keeps S alive at the same address after projection.
            // The writer and the harvest handle never access or expose S.
            // Even &S with S: Sync could invalidate the cell through interior mutability.
            buffer: unsafe { self.buffer.as_ref() },
        }
    }
}

impl<T: Send> fmt::Debug for HarvestCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarvestCell").finish_non_exhaustive()
    }
}

impl<S> fmt::Debug for HarvestHandle<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarvestHandle").finish_non_exhaustive()
    }
}

impl<S> fmt::Debug for Writer<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Writer").finish_non_exhaustive()
    }
}

impl<T: Send> fmt::Debug for Borrowed<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Borrowed").finish_non_exhaustive()
    }
}

impl<T: Send, S: Send> fmt::Debug for Owned<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Owned").finish_non_exhaustive()
    }
}
