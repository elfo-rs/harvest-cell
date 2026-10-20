use std::{fmt, time::Duration};

use sealed::sealed;

use crate::{Borrowed, HarvestHandle, Storage, barrier};

/// Harvests one buffer from each supplied cell with a single memory barrier.
///
/// After [`start`](Self::start), each cell has a pending harvest: its previous
/// buffer waits for the callback. Dropping the harvester keeps unprocessed
/// harvests pending. Passing their handles to `start` resumes harvesting
/// without switching those cells again.
///
/// # Examples
///
/// ```
/// use harvest_cell::{HarvestCell, Harvester};
///
/// let mut cell = HarvestCell::new([Vec::<u64>::new(), Vec::new()]);
/// let (mut borrowed_writer, mut borrowed_handle) = cell.split();
/// let (mut owned_writer, mut owned_handle) =
///     HarvestCell::new([Vec::new(), Vec::new()]).into_split();
///
/// borrowed_writer.write(|values| values.push(1));
/// owned_writer.write(|values| values.push(2));
///
/// let handles = [borrowed_handle.as_borrowed(), owned_handle.as_borrowed()];
/// let mut harvester = Harvester::start(handles);
/// let mut total = 0;
/// while let Some(values) = harvester.next_with(std::mem::take) {
///     total += values.into_iter().sum::<u64>();
/// }
/// assert_eq!(total, 3);
/// ```
#[must_use = "call `try_harvest`, `next_with` or `async_next_with` to process the buffers"]
pub struct Harvester<'a, T: Send> {
    pending: Vec<HarvestHandle<Borrowed<'a, T>>>,
    cursor: usize,
}

impl<'a, T: Send> Harvester<'a, T> {
    /// Switches each supplied cell to the other buffer for subsequent writes,
    /// then applies a single memory barrier.
    /// Cells with a pending harvest are not switched again.
    ///
    /// Intended to be called infrequently relative to writes.
    ///
    /// Pass `&mut handle` for one cell, or an iterator of handles such as
    /// `[&mut a, &mut b]` to harvest several together.
    /// See [`IntoHarvestHandles`] for the accepted forms and [`Harvester`]
    /// for an example.
    ///
    /// # Panics
    ///
    /// On Linux with the `membarrier` feature, panics if executing the
    /// registered memory barrier fails.
    pub fn start(handles: impl IntoHarvestHandles<'a, T>) -> Self {
        let pending: Vec<_> = handles
            .into_harvest_handles()
            .map(HarvestHandle::advance)
            .collect();
        if !pending.is_empty() {
            barrier::get().heavy();
        }
        Self { pending, cursor: 0 }
    }

    /// Harvests one ready buffer using the callback.
    ///
    /// The buffer of a pending harvest is ready once the writer no longer uses
    /// it. The callback runs only on a ready buffer and is responsible for
    /// preparing it for reuse by the writer. It can drain or clear the buffer,
    /// or use [`std::mem::take`] to take its contents.
    ///
    /// Returns [`HarvestResult::Item`] with the callback's result,
    /// [`HarvestResult::Busy`] if no remaining buffer is ready,
    /// or [`HarvestResult::Done`] once all buffers are processed.
    ///
    /// # Panics
    ///
    /// If the callback panics, the harvest remains pending for another attempt.
    /// Changes made before the panic remain in the buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use harvest_cell::{HarvestCell, Harvester, HarvestResult};
    ///
    /// let mut cell = HarvestCell::new([Vec::new(), Vec::new()]);
    /// let (mut writer, mut handle) = cell.split();
    /// writer.write(|values| values.push(42));
    ///
    /// let mut harvester = Harvester::start(&mut handle);
    /// let mut values = Vec::new();
    /// loop {
    ///     match harvester.try_harvest(std::mem::take) {
    ///         HarvestResult::Item(buffer) => values.extend(buffer),
    ///         HarvestResult::Busy => std::thread::yield_now(),
    ///         HarvestResult::Done => break,
    ///     }
    /// }
    /// assert_eq!(values, [42]);
    /// ```
    pub fn try_harvest<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> HarvestResult<R> {
        if self.pending.is_empty() {
            return HarvestResult::Done;
        }

        // Resume from the previous position, wrapping around to revisit busy
        // buffers.
        let cursor = self.cursor;
        let (head, tail) = self.pending.split_at_mut(cursor);
        let next = tail
            .iter_mut()
            .enumerate()
            .map(|(index, handle)| (cursor + index, handle))
            .chain(head.iter_mut().enumerate())
            .find(|(_, handle)| handle.can_harvest());
        let Some((index, handle)) = next else {
            return HarvestResult::Busy;
        };

        // SAFETY: `Harvester::start()` applied the barrier, and
        // `HarvestHandle::can_harvest()` confirmed that the writer is not using
        // this buffer. This handle holds the single harvester right for this
        // cell.
        let result = unsafe { handle.harvest(f) };
        self.pending.swap_remove(index);
        self.cursor = index;
        HarvestResult::Item(result)
    }

    /// Waits for a ready buffer and harvests it using the callback.
    ///
    /// The callback is responsible for preparing the buffer for reuse by the
    /// writer. See [`try_harvest`](Self::try_harvest) for details.
    ///
    /// Returns `None` when all buffers have been processed.
    ///
    /// Uses a simple backoff strategy that assumes writes are short.
    /// For longer writes or different waiting requirements, use
    /// [`try_harvest`](Self::try_harvest) in a loop with your own backoff
    /// strategy.
    ///
    /// # Panics
    ///
    /// Callback panics behave as in [`try_harvest`](Self::try_harvest).
    pub fn next_with<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        let mut f = Some(f);
        let mut backoff = Backoff::default();
        loop {
            match self.try_harvest(|buffer| f.take().unwrap()(buffer)) {
                HarvestResult::Item(result) => return Some(result),
                HarvestResult::Done => return None,
                HarvestResult::Busy => {}
            }

            match backoff.step() {
                BackoffStep::Spin(iterations) => {
                    for _ in 0..iterations {
                        std::hint::spin_loop();
                    }
                }
                BackoffStep::Yield => std::thread::yield_now(),
                BackoffStep::Sleep(duration) => std::thread::sleep(duration),
            }
        }
    }

    /// Waits for a ready buffer and harvests it using the callback.
    ///
    /// The callback is responsible for preparing the buffer for reuse by the
    /// writer. See [`try_harvest`](Self::try_harvest) for details.
    ///
    /// Returns `None` when all buffers have been processed.
    ///
    /// Uses a simple backoff strategy that assumes writes are short.
    /// For longer writes or different waiting requirements, use
    /// [`try_harvest`](Self::try_harvest) in a loop with your own backoff
    /// strategy.
    ///
    /// # Cancel safety
    ///
    /// Cancelling while waiting leaves the harvest pending for a later attempt.
    ///
    /// # Panics
    ///
    /// Callback panics behave as in [`try_harvest`](Self::try_harvest).
    ///
    /// Panics if a timed retry is needed without a Tokio runtime with time
    /// enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// use harvest_cell::{HarvestCell, Harvester};
    ///
    /// # tokio::runtime::Builder::new_current_thread()
    /// #     .enable_time().build().unwrap().block_on(async {
    /// let (mut writer, mut handle) =
    ///     HarvestCell::new([Vec::new(), Vec::new()]).into_split();
    /// writer.write(|values| values.push(42));
    ///
    /// let mut harvester = Harvester::start(&mut handle);
    /// let mut values = Vec::new();
    /// while let Some(buffer) = harvester.async_next_with(std::mem::take).await {
    ///     values.extend(buffer);
    /// }
    /// assert_eq!(values, [42]);
    /// # });
    /// ```
    #[cfg(feature = "tokio")]
    pub async fn async_next_with<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        let mut f = Some(f);
        let mut backoff = Backoff::default();
        loop {
            match self.try_harvest(|buffer| f.take().unwrap()(buffer)) {
                HarvestResult::Item(result) => return Some(result),
                HarvestResult::Done => return None,
                HarvestResult::Busy => {}
            }

            match backoff.step() {
                BackoffStep::Spin(iterations) => {
                    for _ in 0..iterations {
                        std::hint::spin_loop();
                    }
                }
                BackoffStep::Yield => tokio::task::yield_now().await,
                BackoffStep::Sleep(duration) => tokio::time::sleep(duration).await,
            }
        }
    }
}

/// Handles accepted by [`Harvester::start()`]: `&mut handle`, a borrowed
/// handle, or an iterator of either, such as `[&mut a, &mut b]`.
#[sealed]
pub trait IntoHarvestHandles<'a, T: Send + 'a> {
    #[doc(hidden)]
    fn into_harvest_handles(self) -> impl Iterator<Item = HarvestHandle<Borrowed<'a, T>>>;
}

#[sealed]
impl<'a, S: Storage> IntoHarvestHandles<'a, S::Buffer> for &'a mut HarvestHandle<S> {
    #[inline]
    fn into_harvest_handles(self) -> impl Iterator<Item = HarvestHandle<Borrowed<'a, S::Buffer>>> {
        std::iter::once(self.as_borrowed())
    }
}

#[sealed]
impl<'a, T: Send> IntoHarvestHandles<'a, T> for HarvestHandle<Borrowed<'a, T>> {
    #[inline]
    fn into_harvest_handles(self) -> impl Iterator<Item = Self> {
        std::iter::once(self)
    }
}

#[sealed]
impl<'a, T: Send + 'a, I> IntoHarvestHandles<'a, T> for I
where
    I: IntoIterator,
    I::Item: Into<HarvestHandle<Borrowed<'a, T>>>,
{
    #[inline]
    fn into_harvest_handles(self) -> impl Iterator<Item = HarvestHandle<Borrowed<'a, T>>> {
        self.into_iter().map(Into::into)
    }
}

/// Reports the result of a harvest attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum HarvestResult<R> {
    /// The callback's result for one harvested buffer.
    Item(R),
    /// No remaining buffer is ready for harvesting.
    Busy,
    /// All buffers in this harvest are processed.
    Done,
}

impl<T: Send> fmt::Debug for Harvester<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Harvester")
            .field("pending", &self.pending.len())
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

// Spin for 2, 4, 8, 16, then 32 iterations; follow with 7 rounds of
// 32 spins and a yield. Sleep for 1 ms, then restart the strategy.
#[derive(Default)]
struct Backoff {
    step: u32,
}

enum BackoffStep {
    Spin(u32),
    Yield,
    Sleep(Duration),
}

impl Backoff {
    fn step(&mut self) -> BackoffStep {
        const SPIN: u32 = 5;
        const SPIN_WITH_YIELD: u32 = 7 * 2;
        const LIMIT: u32 = SPIN + SPIN_WITH_YIELD;
        const SLEEP_DURATION: Duration = Duration::from_millis(1);

        let step = self.step;
        self.step += 1;

        if step >= LIMIT {
            self.step = 0;
            return BackoffStep::Sleep(SLEEP_DURATION);
        }

        if step < SPIN {
            return BackoffStep::Spin(1 << (step + 1));
        }

        let yield_now = (step - SPIN) % 2 != 0;
        if !yield_now {
            return BackoffStep::Spin(1 << SPIN);
        }

        BackoffStep::Yield
    }
}
