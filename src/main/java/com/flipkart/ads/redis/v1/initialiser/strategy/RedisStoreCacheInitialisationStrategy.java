package com.flipkart.ads.redis.v1.initialiser.strategy;

import com.flipkart.ads.redis.v1.cache.RedisDataInMemoryCache;
import com.flipkart.ads.redis.v1.datastore.DataStore;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class RedisStoreCacheInitialisationStrategy implements RedisInitialisationStrategy<RedisDataInMemoryCache> {
    private final DataStore dataStore;

    @Inject
    public RedisStoreCacheInitialisationStrategy(DataStore<?> dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void initialise(Boolean isLeader, RedisDataInMemoryCache cacheObject) {
        try {
            cacheObject.init();
            log.info("Initialising Redis Data Store Cache");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void putInCache(RedisMap cacheName, RedisDataInMemoryCache redisDataStoreCache) {
        dataStore.putCache(cacheName, redisDataStoreCache);
    }

    @Override
    public void destroy(RedisDataInMemoryCache cacheObject) throws IOException {
        cacheObject.close();
    }
}
