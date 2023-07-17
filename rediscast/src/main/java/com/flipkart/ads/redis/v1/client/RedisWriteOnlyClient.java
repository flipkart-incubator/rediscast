package com.flipkart.ads.redis.v1.client;

import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.exceptions.RedisWriteSynchronizationException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.pool.RedisSynchronizationConfig;

import java.util.function.Consumer;

public interface RedisWriteOnlyClient {
    /**
     * Writes to Redis in a synchronous manner.
     * Every write to master is explicitly replicated to a number of slave as configured in redisSynchronizationConfig
     *
     * @param entity                     data to write in Redis
     * @param finallyCallBack            callback function to execute
     * @param redisSynchronizationConfig config related to making redis write synchronous
     * @throws RedisWriteSynchronizationException
     */
    void updateInRedisSynchronously(RedisEntity<String, Object> entity, Consumer<String> finallyCallBack, RedisSynchronizationConfig redisSynchronizationConfig) throws RedisWriteSynchronizationException;

    /**
     * Delete from Redis in a synchronous manner.
     * Every delete to master is explicitly replicated to a number of slave as configured in redisSynchronizationConfig
     *
     * @param entity                     data to remove from Redis
     * @param finallyCallBack            callback function to execute
     * @param redisSynchronizationConfig config related to making redis remove synchronous
     * @throws RedisWriteSynchronizationException
     */
    void removeFromRedisSynchronously(RedisEntity<String, Object> entity, Consumer<String> finallyCallBack, RedisSynchronizationConfig redisSynchronizationConfig) throws RedisWriteSynchronizationException;

    /**
     * Given a fully qualified key, check if the key is present in redis
     *
     * @param key Fully qualified key to check
     * @return boolean value which represent whether key is present
     * @throws RedisDataStoreException
     */
    boolean containsKey(String key) throws RedisDataStoreException;

    /**
     * Given a mapName and entityId, check if the id is present in redis
     *
     * @param mapName name of the map in which the key belongs
     * @param key     entity key
     * @return boolean value which represent whether key is present
     * @throws RedisDataStoreException
     */
    boolean containsKey(String mapName, String key) throws RedisDataStoreException;
}
