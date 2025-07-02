#pragma once

#include <string>
#include <atomic>

namespace container {
    class SpinLock {
    public:
        std::atomic<bool> is_locked{};
        explicit SpinLock() = default;

        void lock();
        void unlock();
        bool try_lock();
    };

    class SpinLockGuard {
    private:
        SpinLock& lock;
    public:
        explicit SpinLockGuard(SpinLock& lock);
        ~SpinLockGuard();
    };
}