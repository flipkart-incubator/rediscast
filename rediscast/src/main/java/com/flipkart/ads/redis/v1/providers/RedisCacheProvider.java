package com.flipkart.ads.redis.v1.providers;

import com.flipkart.ads.redis.v1.model.RedisMap;

import java.util.Map;

public interface RedisCacheProvider {
    Map<RedisMap, Object> provideCaches();
}
