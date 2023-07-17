package com.flipkart.ads.redis.v1.exceptions;

public class RedisDataStoreEventListenerException extends Exception {
    public RedisDataStoreEventListenerException(String message) {
        super(message);
    }

    public RedisDataStoreEventListenerException(String message, Throwable cause) {
        super(message, cause);
    }
}
