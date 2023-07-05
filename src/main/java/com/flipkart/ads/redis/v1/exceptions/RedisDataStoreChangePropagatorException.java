package com.flipkart.ads.redis.v1.exceptions;

public class RedisDataStoreChangePropagatorException extends Exception {
    public RedisDataStoreChangePropagatorException(String msg) {
        super(msg);
    }

    public RedisDataStoreChangePropagatorException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
