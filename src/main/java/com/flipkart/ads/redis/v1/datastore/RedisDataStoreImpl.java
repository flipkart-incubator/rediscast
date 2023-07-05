package com.flipkart.ads.redis.v1.datastore;

import com.flipkart.ads.redis.v1.cache.RedisDataInMemoryCache;
import com.flipkart.ads.redis.v1.exceptions.RedisInMemoryCacheException;
import com.flipkart.ads.redis.v1.model.RedisMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class RedisDataStoreImpl implements DataStore<RedisDataInMemoryCache> {
    private final Map<RedisMap, RedisDataInMemoryCache> inMemoryMapsCaches = new ConcurrentHashMap<>();

    @Override
    public <T> T get(RedisMap map, String key) {
        RedisDataInMemoryCache mapCache = inMemoryMapsCaches.get(map);
        if (mapCache == null) {
            log.error("Error in reading from ingestion map:{}", map.getMapName());
        } else {
            try {
                return mapCache.get(key);
            } catch (RedisInMemoryCacheException ex) {
                log.error("Error in reading from ingestion map:{} for key: {}", map.getMapName(), key);
            }
        }
        return null;
    }

    @Override
    public <T> Map<String, T> mGet(RedisMap map, List<String> keys) {
        RedisDataInMemoryCache mapCache = inMemoryMapsCaches.get(map);
        if (mapCache == null) {
            log.error("Error in reading from ingestion map:{}", map.getMapName());
        } else {
            try {
                return mapCache.mGet(keys);
            } catch (RedisInMemoryCacheException ex) {
                log.error("Error in reading from ingestion map:{} for key: {}", map.getMapName(), keys);
            }
        }
        return new HashMap<>();
    }

    @Override
    public <T> Collection<T> getAll(RedisMap map) {
        RedisDataInMemoryCache mapCache = inMemoryMapsCaches.get(map);

        if (mapCache == null) {
            log.error("Error in reading from ingestion map:{}", map.getMapName());
            return null;
        } else {
            return mapCache.values();
        }
    }

    @Override
    public RedisDataInMemoryCache get(RedisMap map) {
        return inMemoryMapsCaches.get(map);
    }

    @Override
    public boolean isDataStoreInitialised(RedisMap map) {
        return inMemoryMapsCaches.get(map).isInitialized();
    }

    @Override
    public void putCache(RedisMap cacheName, RedisDataInMemoryCache ingestionDataStoreCache) {
        inMemoryMapsCaches.put(cacheName, ingestionDataStoreCache);
    }
}
