package com.flipkart.ads.redis.v1.exceptions;

public class RedisDataStoreException extends Exception {
    public RedisDataStoreException(String msg) {
        super(msg);
    }

    public RedisDataStoreException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
