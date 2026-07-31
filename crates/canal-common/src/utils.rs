use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Extension trait to recover from a poisoned Mutex or RwLock.
/// When a lock is poisoned, continuing with the inner state
/// is the best available option — the invariant is already broken.
pub trait LockExt<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T>;
    fn read_or_recover(&self) -> RwLockReadGuard<'_, T>;
    fn write_or_recover(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> LockExt<T> for Mutex<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn read_or_recover(&self) -> RwLockReadGuard<'_, T> {
        unreachable!("read_or_recover is not valid for Mutex; use lock_or_recover")
    }

    fn write_or_recover(&self) -> RwLockWriteGuard<'_, T> {
        unreachable!("write_or_recover is not valid for Mutex; use lock_or_recover")
    }
}

impl<T> LockExt<T> for RwLock<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T> {
        unreachable!(
            "lock_or_recover is not valid for RwLock; use read_or_recover or write_or_recover"
        )
    }

    fn read_or_recover(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_or_recover(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(PoisonError::into_inner)
    }
}
