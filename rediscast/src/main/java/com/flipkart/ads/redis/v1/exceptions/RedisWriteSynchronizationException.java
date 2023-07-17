package com.flipkart.ads.redis.v1.exceptions;

public class RedisWriteSynchronizationException extends Exception {
    public RedisWriteSynchronizationException(String msg) {
        super(msg);
    }

    public RedisWriteSynchronizationException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
