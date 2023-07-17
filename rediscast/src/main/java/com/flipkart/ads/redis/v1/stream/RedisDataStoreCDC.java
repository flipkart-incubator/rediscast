package com.flipkart.ads.redis.v1.stream;

import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.model.RedisEntity;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface RedisDataStoreCDC extends Stoppable {
    /**
     * @param mapName  datastore mapName/table/map
     * @param listener event callback receiver
     * @return listener id which can be used to remove the listener
     * With this method client can only have one listener per mapName in an machine.
     */
    String addListener(String mapName, RedisDataStoreChangePropagator<String, Object> listener)
            throws RedisDataStoreChangePropagatorException;

    /**
     * @param mapName        datastore mapName/table/map
     * @param listener       event callback receiver
     * @param initialDelayMs initial delay to start listener
     * @param frequencyMs    period after next invocation should happen
     * @return listener id which can be used to remove the listener
     * @throws RedisDataStoreChangePropagatorException
     */
    String addListener(String mapName, RedisDataStoreChangePropagator<String, Object> listener, int initialDelayMs, int frequencyMs)
            throws RedisDataStoreChangePropagatorException;

    /**
     * @param listenerId remove listener by listener id
     * @return
     * @throws IOException
     */
    boolean removeListener(String listenerId) throws RedisDataStoreChangePropagatorException;

    /**
     * @param mapName
     * @return true if mapName is empty, false otherwise
     */
    boolean isEmpty(String mapName) throws RedisDataStoreException;


    /**
     * @param mapName
     * @return size of mapName, total number of KV in the mapName
     */
    long size(String mapName) throws RedisDataStoreException;

    /**
     * @param mapName
     * @param key     key to be searched in the mapName
     * @return true if mapName contains the key, false otherwise
     */
    boolean containsKey(String mapName, String key) throws RedisDataStoreException;

    /**
     * @param mapName
     * @param key
     * @return value for the key in the mapName, null if mapName doesn't have this key
     */
    RedisEntity<String, Object> get(String mapName, String key) throws RedisDataStoreException;

    /**
     * @param mapName
     * @param key
     * @param eventTime
     * @return value for the key & eventTime in the mapName, null if mapName doesn't have this key
     * @throws RedisDataStoreException
     */
    RedisEntity<String, Object> get(String mapName, String key, long eventTime) throws RedisDataStoreException;

    /**
     * @param mapName
     * @param keys
     * @return Value for all input keys in the mapName, empty map if mapName doesn't have any key.
     */
    Map<String, RedisEntity<String, Object>> getBatch(String mapName, Set<String> keys) throws RedisDataStoreException;

    /**
     * @param mapName
     * @return Set of keys for input mapName.
     */
    Set<String> keySet(String mapName) throws RedisDataStoreException;

    /**
     * @param mapName
     * @return All key-value in present in the mapName
     */
    Map<String, RedisEntity<String, Object>> getAll(String mapName) throws RedisDataStoreException;

    /**
     * @param mapName name of mapName to be checked if created or not
     * @return true if exists false if not
     * @throws - RedisDataStoreException
     */
    boolean mapNameExists(String mapName) throws RedisDataStoreException;
}
