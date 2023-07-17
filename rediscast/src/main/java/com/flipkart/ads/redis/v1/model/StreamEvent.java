package com.flipkart.ads.redis.v1.model;

import lombok.Getter;

@Getter
public class StreamEvent<K, V> extends RedisEntity<K, V> {
    private final StreamEventType eventType;

    public StreamEvent(StreamEventType eventType,
                       String entityType,
                       K key,
                       long eventTime) {
        super(entityType, key, eventTime);
        this.eventType = eventType;
    }

    public enum StreamEventType {
        ADD,
        UPDATE,
        DELETE
    }
}
