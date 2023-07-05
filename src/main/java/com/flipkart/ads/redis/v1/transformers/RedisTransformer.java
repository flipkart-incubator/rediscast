package com.flipkart.ads.redis.v1.transformers;

import com.flipkart.ads.redis.v1.exceptions.RedisDataTransformerException;

public interface RedisTransformer<INPUT, OUTPUT> {
    String apply(INPUT data) throws RedisDataTransformerException;

    OUTPUT revert(String data) throws RedisDataTransformerException;
}
