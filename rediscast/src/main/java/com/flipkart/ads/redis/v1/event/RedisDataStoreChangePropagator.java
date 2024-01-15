package com.flipkart.ads.redis.v1.event;

import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.model.RedisEntity;

public interface RedisDataStoreChangePropagator<K, V> {
    /**
     * @param event of new entry added
     */
    void entryAdded(RedisEntity<K, V> event) throws RedisDataStoreChangePropagatorException;

    /**
     * @param event of old entry updated
     */
    void entryUpdated(RedisEntity<K, V> event) throws RedisDataStoreChangePropagatorException;

    /**
     * @param event of entry deleted
     */
    void entryDeleted(RedisEntity<K, V> event) throws RedisDataStoreChangePropagatorException;
}
