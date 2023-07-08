package com.flipkart.ads.redis.v1.locks;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class MapBasedMultiLock implements MultiLock {
    private final Map<Object, AtomicBoolean> locks = new ConcurrentHashMap<>();
    private final boolean ACQUIRED_LOCK_STATE = true;
    private final long RETRY_AFTER_IN_NANO = 100L;

    public MapBasedMultiLock() {
    }

    @Override
    public void lock(@NonNull Object key) {
        if (!this.tryLock(key)) {
            AtomicBoolean lock = (AtomicBoolean) this.locks.get(key);

            while (true) {
                boolean isAcquired = lock.compareAndSet(false, true);
                if (isAcquired) {
                    break;
                }

                LockSupport.parkNanos(100L);
            }
        }
    }

    @Override
    public boolean tryLock(@NonNull Object key) {
        return this.acquireIfFirstTime(key) || ((AtomicBoolean) this.locks.get(key)).compareAndSet(false, true);
    }

    @Override
    public void unlock(@NonNull Object key) {
        ((AtomicBoolean) this.locks.get(key)).compareAndSet(true, false);
    }

    private boolean acquireIfFirstTime(@NonNull Object key) {
        return this.locks.putIfAbsent(key, new AtomicBoolean(true)) == null;
    }
}
