package com.flipkart.ads.redis.v1.exceptions;

public class RedisDataTransformerException extends Exception {
    public RedisDataTransformerException(String msg) {
        super(msg);
    }

    public RedisDataTransformerException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
