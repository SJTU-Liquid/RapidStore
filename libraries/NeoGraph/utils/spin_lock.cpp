#include "spin_lock.h"

namespace container {
    void SpinLock::lock() {
        while (is_locked.exchange(true, std::memory_order_acquire)) {
        }
    }

    void SpinLock::unlock() {
        is_locked.store(false, std::memory_order_release);
    }
    
    bool SpinLock::try_lock() {
        return !is_locked.exchange(true, std::memory_order_acquire);
    }

    SpinLockGuard::SpinLockGuard(SpinLock &lock): lock(lock) {
        lock.lock();
    }

    SpinLockGuard::~SpinLockGuard() {
        lock.unlock();
    }
}