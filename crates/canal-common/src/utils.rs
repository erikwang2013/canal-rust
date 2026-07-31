use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Extension trait to recover from a poisoned Mutex.
/// When a lock is poisoned, continuing with the inner state
/// is the best available option — the invariant is already broken.
pub trait MutexLockExt<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexLockExt<T> for Mutex<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|e| {
            tracing::error!("Recovered from poisoned Mutex — data may be inconsistent");
            PoisonError::into_inner(e)
        })
    }
}

/// Extension trait to recover from a poisoned RwLock.
pub trait RwLockExt<T> {
    fn read_or_recover(&self) -> RwLockReadGuard<'_, T>;
    fn write_or_recover(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn read_or_recover(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(|e| {
            tracing::error!("Recovered from poisoned RwLock — data may be inconsistent");
            PoisonError::into_inner(e)
        })
    }

    fn write_or_recover(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(|e| {
            tracing::error!("Recovered from poisoned RwLock — data may be inconsistent");
            PoisonError::into_inner(e)
        })
    }
}
