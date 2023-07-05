package com.flipkart.ads.redis.v1.locks;

import com.google.common.util.concurrent.Striped;
import lombok.NonNull;

import java.util.concurrent.locks.Lock;

public class StripedMultiLock implements MultiLock {
    private final Striped<Lock> locks;

    public StripedMultiLock(int stripes, boolean isLazyInit) {
        this.locks = isLazyInit ? Striped.lazyWeakLock(stripes) : Striped.lock(stripes);
    }

    @Override
    public void lock(@NonNull Object key) {
        ((Lock) this.locks.get(key)).lock();
    }

    @Override
    public boolean tryLock(@NonNull Object key) {
        return ((Lock) this.locks.get(key)).tryLock();
    }

    @Override
    public void unlock(@NonNull Object key) {
        ((Lock) this.locks.get(key)).unlock();
    }
}
