package com.flipkart.ads.redis.v1.datastore;

import com.flipkart.ads.redis.v1.model.RedisMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface DataStore<CacheType> {
    <T> T get(RedisMap map, String key);

    <T> Map<String, T> mGet(RedisMap map, List<String> keys);

    <T> Collection<T> getAll(RedisMap map);

    CacheType get(RedisMap map);

    boolean isDataStoreInitialised(RedisMap map);

    void putCache(RedisMap cacheName, CacheType ingestionDataStoreCache);
}
