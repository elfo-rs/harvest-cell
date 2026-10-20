#[cfg(all(harvest_cell_loom, feature = "loom"))]
pub(crate) use loom::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering, fence},
    },
};

#[cfg(not(all(harvest_cell_loom, feature = "loom")))]
pub(crate) use self::standard::*;

#[cfg(not(all(harvest_cell_loom, feature = "loom")))]
mod standard {
    pub(crate) use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering, fence},
    };

    #[repr(transparent)]
    pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        #[inline]
        pub(crate) fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }

        #[inline]
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}
