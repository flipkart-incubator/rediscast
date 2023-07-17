package com.flipkart.ads.redis.v1.cache;

public interface TTLExpiryListener<K, V> {
    void ttlExpired(K key, V value);
}
