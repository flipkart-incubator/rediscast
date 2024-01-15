package com.flipkart.ads.redis.v1.initialiser.strategy;

import com.flipkart.ads.redis.v1.model.RedisMap;

public interface RedisInitialisationStrategy<CacheType> {
    void initialise(Boolean isLeader, CacheType cacheObject);

    void putInCache(RedisMap cacheName, CacheType cacheObject);

    void destroy(CacheType cacheObject) throws Exception;
}
