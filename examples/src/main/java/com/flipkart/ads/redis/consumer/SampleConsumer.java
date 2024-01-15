package com.flipkart.ads.redis.consumer;

import com.flipkart.ads.redis.v1.initialiser.RedisInitialiser;

public class SampleConsumer {
    //Redis Initializer is the starting point to consume the events. If using dropwizard guice, the instance will be available at runtime to inject if the configs are provided
    // No need to create it explicitly
    private final RedisInitialiser redisInitialiser;

    public SampleConsumer(RedisInitialiser redisInitialiser) {
        this.redisInitialiser = redisInitialiser;
    }

    // Start consumption
    public void startConsumption() {
        redisInitialiser.initialise(true);
    }
}
