package com.flipkart.ads.redis.v1.transformers;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class StringRedisTransformer implements RedisTransformer<String, String> {
    @Override
    public String apply(String data) {
        return data;
    }

    @Override
    public String revert(String data) {
        return data;
    }
}
