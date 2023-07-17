package com.flipkart.ads.redis.v1.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.Setter;

import java.io.Serializable;

@Data
public class RedisEntity<K, V> implements Serializable {
    private final String type;
    private final K key;
    private final long eventTime;
    @Setter
    private V value;
    @Setter
    private long ttl;
    @Setter
    private String mapName;

    @JsonCreator
    public RedisEntity(@JsonProperty("type") String type,
                       @JsonProperty("key") K key,
                       @JsonProperty("eventTime") long eventTime) {
        this.type = type;
        this.key = key;
        this.eventTime = eventTime;
    }

    @JsonIgnore
    public RedisEntity<K, V> getShallowCopy() {
        RedisEntity<K, V> redisEntityCopy = new RedisEntity<>(type, key, eventTime);

        redisEntityCopy.setValue(value);
        redisEntityCopy.setTtl(ttl);
        redisEntityCopy.setMapName(mapName);

        return redisEntityCopy;
    }
}
