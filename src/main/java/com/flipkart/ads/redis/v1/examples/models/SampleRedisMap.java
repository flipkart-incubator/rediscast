package com.flipkart.ads.redis.v1.examples.models;

import com.flipkart.ads.redis.v1.model.RedisMap;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum SampleRedisMap implements RedisMap {
    SAMPLE_MAP_ONE("sampleMapOne"),
    SAMPLE_MAP_TWO("sampleMapTwo"),
    SAMPLE_MAP_THREE("sampleMapThree");

    private final String projectionName;

    @Override
    public String getMapName() {
        return projectionName;
    }
}
