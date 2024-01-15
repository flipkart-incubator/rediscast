package com.flipkart.ads.redis.v1.stream;

import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;

public interface Stoppable {
    boolean stop() throws RedisDataStoreException;
}
