#[cfg(all(
    feature = "membarrier",
    target_os = "linux",
    not(any(miri, all(harvest_cell_loom, feature = "loom")))
))]
pub(crate) use membarrier::{Barrier, get};

#[cfg(not(all(
    feature = "membarrier",
    target_os = "linux",
    not(any(miri, all(harvest_cell_loom, feature = "loom")))
)))]
pub(crate) use fallback::{Barrier, get};

mod fallback {
    use crate::sync::{Ordering, fence};

    /// Pairs a light barrier on one thread with a heavy barrier on another to
    /// order memory accesses as two `SeqCst` fences would.
    #[derive(Clone, Copy)]
    pub(crate) struct Barrier;

    /// Returns the barrier for this process.
    #[inline]
    pub(crate) fn get() -> Barrier {
        Barrier
    }

    #[allow(clippy::unused_self)]
    impl Barrier {
        /// Issues the light barrier for the fast path.
        #[inline]
        pub(crate) fn light(self) {
            fence(Ordering::SeqCst);
        }

        /// Issues the heavy barrier for the slow path.
        #[inline]
        pub(crate) fn heavy(self) {
            fence(Ordering::SeqCst);
        }
    }
}

#[cfg(all(
    feature = "membarrier",
    target_os = "linux",
    not(any(miri, all(harvest_cell_loom, feature = "loom")))
))]
mod membarrier {
    use std::sync::{
        OnceLock,
        atomic::{Ordering, compiler_fence},
    };

    use super::fallback;

    /// Pairs a light barrier on one thread with a heavy barrier on another to
    /// order memory accesses as two `SeqCst` fences would.
    #[derive(Clone, Copy)]
    pub(crate) enum Barrier {
        Fence(fallback::Barrier),
        PrivateExpedited,
    }

    /// Returns the barrier for this process.
    #[inline]
    pub(crate) fn get() -> Barrier {
        // Keep the same implementation for both sides throughout the process.
        static BARRIER: OnceLock<Barrier> = OnceLock::new();

        *BARRIER.get_or_init(|| {
            if syscall(libc::MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED) {
                Barrier::PrivateExpedited
            } else {
                Barrier::Fence(fallback::get())
            }
        })
    }

    impl Barrier {
        /// Issues the light barrier for the fast path.
        #[inline]
        pub(crate) fn light(self) {
            match self {
                Self::Fence(barrier) => barrier.light(),
                Self::PrivateExpedited => compiler_fence(Ordering::SeqCst),
            }
        }

        /// Issues the heavy barrier for the slow path.
        ///
        /// # Panics
        ///
        /// If the `membarrier` syscall fails.
        #[inline]
        #[track_caller]
        pub(crate) fn heavy(self) {
            match self {
                Self::Fence(barrier) => barrier.heavy(),
                Self::PrivateExpedited => assert!(
                    syscall(libc::MEMBARRIER_CMD_PRIVATE_EXPEDITED),
                    "membarrier(PRIVATE_EXPEDITED) failed: {}",
                    std::io::Error::last_os_error(),
                ),
            }
        }
    }

    fn syscall(command: libc::c_int) -> bool {
        // SAFETY: The kernel rejects unknown commands.
        unsafe {
            libc::syscall(
                libc::SYS_membarrier,
                command,
                0 as libc::c_uint,
                0 as libc::c_int,
            ) == 0
        }
    }
}
