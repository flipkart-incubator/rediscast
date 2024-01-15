package com.flipkart.ads.redis.v1.cache;

import com.flipkart.ads.redis.v1.exceptions.RedisInMemoryCacheException;
import lombok.NonNull;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisDataInMemoryCache extends Closeable {
    String getMapName();

    void init() throws Exception;

    boolean isInitialized();

    <K, V> V get(K key) throws RedisInMemoryCacheException;

    <K, V> Map<K, V> mGet(@NonNull List<K> keys) throws RedisInMemoryCacheException;

    <V> Collection<V> values() throws RedisInMemoryCacheException;

    long size() throws RedisInMemoryCacheException;

    <K, V> Set<Map.Entry<K, V>> entrySet() throws RedisInMemoryCacheException;
}
