package com.flipkart.ads.redis.v1.client;

import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.model.StreamEvent;
import com.flipkart.ads.redis.v1.model.StreamEventBatch;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisReadOnlyClient {
    /**
     * Checks whether the map exists in redis
     * @param mapName map name to check
     * @return returns true if given map is present
     */
    boolean mapNameExists(String mapName);

    /**
     * Checks whether a given keys is present in the given map in redis
     * @param mapName name of the map in which to check if key is present
     * @param key key name to check
     * @return returns true if key is exists in the given map
     * @throws RedisDataStoreException
     */
    boolean containsKey(String mapName, String key) throws RedisDataStoreException;

    /**
     * Gives number of entries present in the given map
     * @param mapName map name
     * @return return count of the entries present in the map
     */
    long size(String mapName) throws RedisDataStoreException;

    /**
     * Get the data of the given key & map name
     * @param mapName map name
     * @param key key
     * @return returns the data of the given key
     * @throws RedisDataStoreException
     */
    RedisEntity<String, Object> get(String mapName, String key) throws RedisDataStoreException;

    /**
     * Get the data for the given key & map if the data version of the keys is >= to the given version(eventTime)
     * If the version of the data present in the redis less than given version, it returns null
     * @param mapName map name
     * @param key key
     * @param eventTime version of entry
     * @return returns data of the given key
     * @throws RedisDataStoreException
     */
    RedisEntity<String, Object> get(String mapName, String key, long eventTime) throws RedisDataStoreException;


    /**
     * Get the entry's data for the given map & keys set
     * @param mapName map name
     * @param keys keys to get the data
     * @return returns map of the given key's data
     * @throws RedisDataStoreException
     */
    Map<String, RedisEntity<String, Object>> getBatch(String mapName, Set<String> keys) throws RedisDataStoreException;

    /**
     * Get all the entry's data present in the given map
     * @param mapName map name
     * @return returns data of all the keys present in the map
     * @throws RedisDataStoreException
     */
    Map<String, RedisEntity<String, Object>> getAll(String mapName) throws RedisDataStoreException;

    /**
     * Get the next batch of events for the given map name for the given stream id
     * @param mapName map name
     * @param streamIdTracker stream id
     * @return returns list of stream events received after the given stream id
     * @throws RedisDataStoreException
     * @throws InterruptedException
     */
    List<StreamEntry> nextBatch(String mapName, StreamEntryID streamIdTracker) throws RedisDataStoreException, InterruptedException;

    /**
     * Get the next batch of events along with each entry's data for the given map name for the given stream id
     * @param mapName map name
     * @param streamIdTracker stream id
     * @return returns list of stream events and its corresponding each entry's data received after the given stream id
     * @throws RedisDataStoreException
     * @throws InterruptedException
     */
    StreamEventBatch<String, Object> nextBatchInStreamEventBatch(String mapName, StreamEntryID streamIdTracker) throws RedisDataStoreException, InterruptedException;

    /**
     * Get the stream event entity's data(including the value of entity) from the given stream event metadata
     * @param fields stream event meta
     * @param mapName map name
     * @param newDataFetch should fetch the value of the key if not present in the stream event metadata
     * @return returns enriched stream event data
     */
    StreamEvent<String, Object> getStreamEvent(Map<String, String> fields, String mapName, boolean newDataFetch);

    /**
     * Get all the keys present in the given map
     * @param mapName map name
     * @return returns set of keys present in the given map
     */
    Set<String> keySet(String mapName);
}
