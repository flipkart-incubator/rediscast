package com.flipkart.ads.redis.v1.exceptions;

public class RedisInMemoryCacheException extends RuntimeException {
    public RedisInMemoryCacheException(String msg) {
        super(msg);
    }

    public RedisInMemoryCacheException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
