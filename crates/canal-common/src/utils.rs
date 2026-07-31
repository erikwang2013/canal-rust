use std::sync::{Mutex, MutexGuard, PoisonError};

/// Extension trait to recover from a poisoned Mutex.
/// When a mutex is poisoned, continuing with the inner state
/// is the best available option — the invariant is already broken.
pub trait LockExt<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> LockExt<T> for Mutex<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
