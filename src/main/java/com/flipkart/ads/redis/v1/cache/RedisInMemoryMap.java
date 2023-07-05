package com.flipkart.ads.redis.v1.cache;

import com.flipkart.ads.redis.v1.exceptions.RedisInMemoryCacheException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.expiringmap.ExpiringMap;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

@Slf4j
public class RedisInMemoryMap {
    private final Map<Object, Long> updateTimeMap;
    private final Map<Object, RedisInMemoryMap.ValueWrapper> copyMap;
    @Getter
    private final RedisInMemoryMap.RedisLocalMapType redisLocalMapType;
    private final String name;
    private TTLExpiryListener<String, Object> ttlExpiryListener;

    public RedisInMemoryMap(RedisInMemoryMap.RedisLocalMapType redisLocalMapType, String name) {
        this.ttlExpiryListener = null;
        this.redisLocalMapType = redisLocalMapType;
        this.name = name;

        if (RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP.equals(redisLocalMapType)) {
            updateTimeMap = new ConcurrentHashMap<>();
            copyMap = null;
        } else if (RedisInMemoryMap.RedisLocalMapType.LOCAL_DATA_MAP.equals(redisLocalMapType)) {
            updateTimeMap = null;
            copyMap = new ConcurrentHashMap<>();
        } else {
            updateTimeMap = null;
            copyMap = ExpiringMap.builder()
                    .variableExpiration()
                    .expiration(365, TimeUnit.DAYS)
                    .asyncExpirationListener((key, object) -> {
                        log.debug("Key expired key: {}", key);
                        if (ttlExpiryListener != null) ttlExpiryListener.ttlExpired((String) key, object);
                    }).build();
        }
    }

    void setTTLExpiryListener(TTLExpiryListener<String, Object> ttlExpiryListener) {
        this.ttlExpiryListener = ttlExpiryListener;
    }

    void checkTTLMapTypeForOperation() {
        if (!redisLocalMapType.equals(RedisInMemoryMap.RedisLocalMapType.LOCAL_DATA_WITH_TTL_SUPPORTED))
            throw new UnsupportedOperationException("TTL is not supported in STANDARD_CONCURRENT_MAP please use map type TTL_SUPPORTED For Local Map: " + name);
    }

    public void update(RedisEntity<String, Object> redisEntity) {
        if (redisLocalMapType.equals(RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP)) {
            updateTimeMap.put(redisEntity.getKey(), redisEntity.getEventTime());
        } else {
            if (redisEntity.getTtl() > 0 && RedisInMemoryMap.RedisLocalMapType.LOCAL_DATA_WITH_TTL_SUPPORTED.equals(redisLocalMapType))
                put(redisEntity.getKey(), createValueWrapper(redisEntity), redisEntity.getTtl());
            else
                copyMap.put(redisEntity.getKey(), createValueWrapper(redisEntity));
        }
    }

    public Object delete(RedisEntity<String, Object> redisEntity) {
        Object value = null;
        if (redisLocalMapType.equals(RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP)) {
            updateTimeMap.remove(redisEntity.getKey());
        } else {
            value = Optional.ofNullable(copyMap.get(redisEntity.getKey())).map(RedisInMemoryMap.ValueWrapper::getEntity).orElse(null);
            if (redisEntity.getTtl() > 0) {
                put(redisEntity.getKey(), createValueWrapper(redisEntity), redisEntity.getTtl());
            } else {
                copyMap.remove(redisEntity.getKey());
            }
        }
        return value;
    }

    void put(Object key, RedisInMemoryMap.ValueWrapper value, long ttlInMs) {
        checkTTLMapTypeForOperation();
        if (ttlInMs > 0)
            ((ExpiringMap<Object, RedisInMemoryMap.ValueWrapper>) copyMap).put(key, value, ttlInMs, MILLISECONDS);
        else
            copyMap.put(key, value);
    }

    public Long getLastKnownTimestamp(Object key) {
        if (redisLocalMapType.equals(RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP))
            return updateTimeMap.getOrDefault(key, -1L);
        else {
            RedisInMemoryMap.ValueWrapper valueWrapper = copyMap.getOrDefault(key, null);
            return valueWrapper == null ? -1L : valueWrapper.getEventTime();
        }
    }

    private void checkLocalCopyMap() throws RedisInMemoryCacheException {
        if (RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP.equals(redisLocalMapType)) {
            throw new RedisInMemoryCacheException("Operation is not allowed when we don't have local copy of data, please use local copy, for Local Map : " + name);
        }
    }

    Object get(Object key) throws RedisInMemoryCacheException {
        checkLocalCopyMap();
        return Optional.ofNullable(copyMap.get(key)).map(RedisInMemoryMap.ValueWrapper::getEntity).orElse(null);
    }

    /**
     * Do we need this capability ??
     **/
    Collection<Object> values() throws RedisInMemoryCacheException {
        checkLocalCopyMap();
        return copyMap.values().stream().map(RedisInMemoryMap.ValueWrapper::getEntity).collect(toList());
    }

    int size() {
        if (RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP.equals(redisLocalMapType))
            return updateTimeMap.size();
        return copyMap.size();
    }

    void remove(Object key) {
        if (RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP.equals(redisLocalMapType))
            updateTimeMap.remove(key);
        else copyMap.remove(key);
    }

    public void clear() {
        if (RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP.equals(redisLocalMapType)) {
            updateTimeMap.clear();
        } else {
            copyMap.clear();
        }
    }

    Set<Map.Entry<Object, RedisInMemoryMap.ValueWrapper>> entrySet() throws RedisInMemoryCacheException {
        checkLocalCopyMap();
        return copyMap.entrySet();
    }

    private RedisInMemoryMap.ValueWrapper createValueWrapper(RedisEntity<String, Object> event) {
        return new ValueWrapper(event.getValue(), event.getEventTime());
    }

    public enum RedisLocalMapType {
        LOCAL_DATA_WITH_TTL_SUPPORTED,
        LOCAL_DATA_MAP,
        MINIMAL_DATA_MAP
    }

    @Getter
    static class ValueWrapper {
        private final Object entity;
        private final long eventTime;

        public ValueWrapper(Object entity, long eventTime) {
            this.entity = entity;
            this.eventTime = eventTime;
        }
    }
}
