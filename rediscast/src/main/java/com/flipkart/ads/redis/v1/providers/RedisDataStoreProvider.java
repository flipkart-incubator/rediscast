package com.flipkart.ads.redis.v1.providers;

import com.flipkart.ads.redis.v1.datastore.DataStore;

public interface RedisDataStoreProvider {
    DataStore<?> provideDataStore();
}
