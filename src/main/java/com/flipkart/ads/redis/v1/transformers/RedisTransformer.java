package com.flipkart.ads.redis.v1.transformers;

import com.flipkart.ads.redis.v1.exceptions.RedisDataTransformerException;

public interface RedisTransformer {
    String apply(Object data) throws RedisDataTransformerException;

    Object revert(String data) throws RedisDataTransformerException;
}
