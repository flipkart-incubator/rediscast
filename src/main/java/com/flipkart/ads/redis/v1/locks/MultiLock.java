package com.flipkart.ads.redis.v1.locks;

public interface MultiLock {
    void lock(Object var1);

    boolean tryLock(Object var1);

    void unlock(Object var1);
}
