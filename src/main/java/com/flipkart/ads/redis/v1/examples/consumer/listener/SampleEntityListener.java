package com.flipkart.ads.redis.v1.examples.consumer.listener;

import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.examples.models.SampleEntity;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.model.RedisEntity;

public class SampleEntityListener implements RedisDataStoreChangePropagator<String, SampleEntity> {
    @Override
    public void entryAdded(RedisEntity<String, SampleEntity> event) throws RedisDataStoreChangePropagatorException {
        // process the event
    }

    @Override
    public void entryUpdated(RedisEntity<String, SampleEntity> event) throws RedisDataStoreChangePropagatorException {
        // process the event

    }

    @Override
    public void entryDeleted(RedisEntity<String, SampleEntity> event) throws RedisDataStoreChangePropagatorException {
        // process the event
    }
}
