package com.flipkart.ads.redis.v1.client;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.exceptions.RedisDataTransformerException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.flipkart.ads.redis.v1.model.StreamEvent;
import com.flipkart.ads.redis.v1.model.StreamEventBatch;
import com.flipkart.ads.redis.v1.pool.JedisSlavePool;
import com.flipkart.ads.redis.v1.transformers.RedisTransformer;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class GenericRedisReadOnlyClient extends AbstractRedisClient implements RedisReadOnlyClient {
    protected final JedisSlavePool jedisSlavePool;
    private final Timer getTimer;
    private final Timer getBasedOnEventTimer;
    private final Timer getBatchTimer;
    private final Timer getNextBatchTimer;
    private final Timer getNextBatchInStreamEventBatchTimer;

    @Inject
    public GenericRedisReadOnlyClient(@Named("redis_stream_slave_pool") JedisSlavePool jedisSlavePool,
                                      Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> mapNameToEntityStreamConfig,
                                      Map<RedisMap, RedisTransformer> mapNameToEntityDataConfig,
                                      MetricRegistry metricRegistry) {
        super(mapNameToEntityStreamConfig, mapNameToEntityDataConfig, jedisSlavePool, metricRegistry);
        this.jedisSlavePool = jedisSlavePool;
        this.getTimer = metricRegistry.timer(MetricRegistry.name(GenericRedisReadOnlyClient.class, "get_timer"));
        this.getBasedOnEventTimer = metricRegistry.timer(MetricRegistry.name(GenericRedisReadOnlyClient.class, "getBasedOnEvent_timer"));
        this.getBatchTimer = metricRegistry.timer(MetricRegistry.name(GenericRedisReadOnlyClient.class, "getBatch_timer"));
        this.getNextBatchTimer = metricRegistry.timer(MetricRegistry.name(GenericRedisReadOnlyClient.class, "getNextBatch_timer"));
        this.getNextBatchInStreamEventBatchTimer = metricRegistry.timer(MetricRegistry.name(GenericRedisReadOnlyClient.class, "getNextBatchInStreamEventBatch_timer"));
    }

    @Timed
    @ExceptionMetered
    @Override
    public boolean mapNameExists(String mapName) {
        return keyExists(jedisSlavePool, getRedisKey(mapName, idsPostFix));
    }

    @Timed
    @ExceptionMetered
    @Override
    public boolean containsKey(String mapName, String key) throws RedisDataStoreException {
        return keyExists(jedisSlavePool, getRedisKey(mapName, key));
    }

    @Override
    public long size(String mapName) {
        try (Jedis resource = jedisSlavePool.getResource()) {
            return resource.scard(getRedisKey(mapName, idsPostFix));
        } catch (Exception e) {
            log.error("Error while checking size of keys in a map {}", mapName, e);
            markExceptionMeter(this.getClass(), "size", mapName);
        }
        return 0;
    }

    @Override
    public RedisEntity<String, Object> get(String mapName, String key) throws RedisDataStoreException {
        Timer.Context timerContext = getTimer.time();
        try (Jedis resource = jedisSlavePool.getResource()) {
            String result = resource.get(getRedisKey(mapName, key));
            if (StringUtils.isBlank(result)) {
                return null;
            }
            Map.Entry<String, RedisEntity<String, Object>> res = getEntityData(mapName, result);
            return res.getValue();
        } catch (IOException | RedisDataTransformerException e) {
            log.error("Exception in get : mapName: {}, e: {}", mapName, e);
            markExceptionMeter(this.getClass(), "get", mapName);
            throw new RedisDataStoreException("Could not fetch data for mapName: " + mapName, e);
        } finally {
            timerContext.stop();
        }
    }

    @Override
    public RedisEntity<String, Object> get(String mapName, String key, long eventTime) throws RedisDataStoreException {
        int retry = 3;
        RedisEntity<String, Object> result = null;
        while (retry > 0) {
            Timer.Context timerContext = getBasedOnEventTimer.time();
            result = get(mapName, key);
            if (result != null) {
                if (result.getEventTime() >= eventTime) {
                    timerContext.stop();
                    return result;
                } else {
                    log.error("Desired version didn't get {} {} {} received {} {}", mapName, key, eventTime, result.getEventTime(), retry);
                }
            }
            retry--;
            timerContext.stop();
        }
        log.error("Got null over version check {} {} {} received {}", mapName, key, eventTime, result != null ? result.getEventTime() : -2);
        return null;
    }

    public Set<String> getMembers(String key) throws RedisDataStoreException {
        try (Jedis resource = jedisSlavePool.getResource()) {
            Set<String> result = resource.smembers(key);
            if (CollectionUtils.isEmpty(result)) {
                return new HashSet<>();
            }
            return result;
        } catch (Exception e) {
            log.error("Exception in getMembers : setName: {}, e: {}", key, e);
            markExceptionMeter(this.getClass(), "getSet", key);
            throw new RedisDataStoreException("Could not fetch data for Set: " + key, e);
        }
    }


    @Override
    public Map<String, RedisEntity<String, Object>> getBatch(String mapName, Set<String> keys) throws RedisDataStoreException {
        Timer.Context timerContext = getBatchTimer.time();
        try (Jedis resource = jedisSlavePool.getResource()) {
            return getBatch(resource, mapName, keys);
        } catch (IOException | RedisDataTransformerException e) {
            log.error("Exception in getBatch : mapName: {}", mapName, e);
            markExceptionMeter(this.getClass(), "getBatch", mapName);
            throw new RedisDataStoreException("Could not fetch data for mapName: " + mapName, e);
        } finally {
            timerContext.stop();
        }
    }

    private Map<String, RedisEntity<String, Object>> getBatch(Jedis resource, String mapName, Set<String> keys) throws IOException, RedisDataTransformerException {
        String[] mGetKeys = keys.stream().map(k -> getRedisKey(mapName, k)).toArray(String[]::new);
        List<String> results = resource.mget(mGetKeys);
        return getBatch(mapName, results, new ArrayList<>(keys));
    }

    private Map<String, RedisEntity<String, Object>> getBatch(String mapName, List<String> results, List<String> keys) throws IOException, RedisDataTransformerException {
        Map<String, RedisEntity<String, Object>> resultMap = new HashMap<>();
        int count = 0;
        for (String result : results) {
            if (StringUtils.isNotBlank(result)) {
                Map.Entry<String, RedisEntity<String, Object>> res = getEntityData(mapName, result);
                resultMap.put(res.getKey(), res.getValue());
            } else {
                log.info("Entry is Empty : {}, count: {} ", mapName, keys.get(count));
            }
            count++;
        }
        log.info("Total entries in map : {}, count: {} ", mapName, count);
        return resultMap;
    }

    @Override
    public Map<String, RedisEntity<String, Object>> getAll(String mapName) throws RedisDataStoreException {
        try (Jedis resource = jedisSlavePool.getResource()) {
            Set<String> keys = keySet(mapName);
            List<String> results = resource.mget(keys.stream().map(k -> getRedisKey(mapName, k)).toArray(String[]::new));
            return getBatch(mapName, results, new ArrayList<>(keys));
        } catch (IOException | RedisDataTransformerException e) {
            log.error("Exception in getAll : mapName: {}, e: {}", mapName, e);
            markExceptionMeter(this.getClass(), "getAll", mapName);
            throw new RedisDataStoreException("Could not fetch data for mapName: " + mapName, e);
        }
    }

    @Override
    public Set<String> keySet(String mapName) {
        try (Jedis resource = jedisSlavePool.getResource()) {
            return resource.smembers(getRedisKey(mapName, idsPostFix));
        }
    }

    @Override
    public List<StreamEntry> nextBatch(String mapName, StreamEntryID streamIdTracker) throws RedisDataStoreException, InterruptedException {
        AbstractRedisClient.EntityStreamConfigs streamConfig = mapNameToEntityStreamConfig.get(mapName);
        if (streamConfig == null) {
            return new ArrayList<>();
        }
        log.debug("Reading Redis Stream Data {} {}", mapName, streamConfig);
        List<Map.Entry<String, List<StreamEntry>>> data = new ArrayList<>();
        Timer.Context timerContext = getNextBatchTimer.time();
        // This is basic redis stream entity which has set of keys like, entityKey, entityType, timeStamp for polling actual data later
        try (Jedis resource = jedisSlavePool.getResource()) {
            Map.Entry<String, StreamEntryID> streamQueries = new AbstractMap.SimpleImmutableEntry<>(streamConfig.getName(), streamIdTracker);
            data.addAll(resource.xread(streamConfig.getCount(), streamConfig.getBlock(), streamQueries));
        } catch (JedisConnectionException | IllegalStateException e) {
            jedisSlavePool.initPool();
            log.error("Re-initiating the pool : {} ", mapName, e);
            Thread.sleep(1000);
            throw new RedisDataStoreException("Resetting pool config: " + mapName, e);
        } catch (Exception e) {
            markExceptionMeter(this.getClass(), "nextBatch", mapName);
            throw new RedisDataStoreException("Could not nextBatch data for mapName: " + mapName, e);
        } finally {
            timerContext.stop();
        }
        for (Map.Entry<String, List<StreamEntry>> datum : data) {
            if (datum.getKey().equals(streamConfig.getName())) {
                return datum.getValue();
            }
        }
        return new ArrayList<>();
    }

    @Override
    public StreamEventBatch<String, Object> nextBatchInStreamEventBatch(String mapName, StreamEntryID streamIdTracker) throws RedisDataStoreException, InterruptedException {
        AbstractRedisClient.EntityStreamConfigs streamConfig = mapNameToEntityStreamConfig.get(mapName);
        if (streamConfig == null) {
            log.debug("No Stream Config found for the given mapName:{}", mapName);
            return null;
        }
        log.debug("Reading Redis Stream Data {} {} {}", mapName, streamConfig.getName(), streamIdTracker.toString());
        Timer.Context timerContext = getNextBatchInStreamEventBatchTimer.time();
        try (Jedis resource = jedisSlavePool.getResource()) {
            Map.Entry<String, StreamEntryID> streamQueries = new AbstractMap.SimpleImmutableEntry<>(streamConfig.getName(), streamIdTracker);
            List<Map.Entry<String, List<StreamEntry>>> data;
            data = resource.xread(streamConfig.getCount(), streamConfig.getBlock(), streamQueries);
            StreamEventBatch<String, Object> streamData = new StreamEventBatch<>(null, streamIdTracker);
            createStreamEvents(mapName, data, streamData);
            enhanceStreamEvents(resource, mapName, streamData);
            return streamData;
        } catch (JedisConnectionException | IllegalStateException e) {
            jedisSlavePool.initPool();
            log.error("Re-initiating the pool : {} ", mapName, e);
            Thread.sleep(1000);
            throw new RedisDataStoreException("Resetting pool config: " + mapName, e);
        } catch (Exception e) {
            markExceptionMeter(this.getClass(), "nextBatch", mapName);
            throw new RedisDataStoreException("Could not nextBatch data for mapName: " + mapName, e);
        } finally {
            timerContext.stop();
        }
    }

    private void enhanceStreamEvents(Jedis resource, String mapName, StreamEventBatch<String, Object> streamData) throws IOException, RedisDataTransformerException {
        if (CollectionUtils.isNotEmpty(streamData.getStreamEventsData())) {
            Set<String> keys = streamData.getStreamEventsData().parallelStream().map(RedisEntity::getKey).collect(Collectors.toSet());
            Map<String, RedisEntity<String, Object>> entities = getBatch(resource, mapName, keys);
            keys.removeAll(entities.keySet());
            if (keys.size() > 0) {
                log.error("Didn't get data for {} - {} {} {} {}", mapName, entities.size(), keys.size(), entities.keySet(), keys);
            }
            streamData.getStreamEventsData().parallelStream()
                    .filter(s -> entities.containsKey(s.getKey()))
                    .forEach(s -> s.setValue(entities.get(s.getKey()).getValue()));
        }
    }

    private void createStreamEvents(String mapName, List<Map.Entry<String, List<StreamEntry>>> data, StreamEventBatch<String, Object> streamData) {
        if (CollectionUtils.isNotEmpty(data)) {
            for (Map.Entry<String, List<StreamEntry>> datum : data) {
                if (datum.getKey().equals(mapNameToEntityStreamConfig.get(mapName).getName()) && CollectionUtils.isNotEmpty(datum.getValue())) {
                    datum.getValue().forEach(entry -> {
                        log.info("StreamId {} Read:{}", datum.getKey(), entry.getID());
                        streamData.setMaxStreamId(entry.getID());
                        streamData.getStreamEventsData().add(getStreamEvent(entry.getFields(), mapName, false));
                    });
                } else {
                    log.error("map name didn't match {} {} {}", mapName, datum.getKey(), mapNameToEntityStreamConfig.get(mapName).getName());
                }
            }
        }
    }

    @Override
    public StreamEvent<String, Object> getStreamEvent(Map<String, String> fields, String mapName, boolean newDataFetch) {
        StreamEvent.StreamEventType eventType = StreamEvent.StreamEventType.valueOf(fields.get(COMMAND).toUpperCase());
        String key = fields.get(ENTITY_KEY);
        long eventTime = -1L;
        if (StringUtils.isNotBlank(fields.get(EVENT_TIME))) {
            eventTime = Long.parseLong(fields.get(EVENT_TIME));
        }
        StreamEvent<String, Object> event = new StreamEvent<>(eventType, mapName, key, eventTime);
        Object entity = null;
        String value = fields.get(ENTITY_VALUE);
        if (StringUtils.isNotBlank(value)) {
            String type = fields.get(ENTITY_TYPE);
            entity = deserializeEntity(type, value);
        } else if (newDataFetch) {
            RedisEntity<String, Object> redisEntity = getEntityValue(key, StreamEvent.StreamEventType.DELETE.equals(eventType) ? -1 : eventTime, mapName);
            if (redisEntity != null) {
                entity = redisEntity.getValue();
            }
        }
        event.setValue(entity);
        return event;
    }

    private RedisEntity<String, Object> getEntityValue(String key, long eventTime, String mapName) {
        int retry = 3;
        while (retry > 0) {
            try {
                return get(mapName, key, eventTime);
            } catch (RedisDataStoreException e) {
                log.error("Stream get entity issue {} {}", mapName, key, e);
                retry--;
            }
        }
        return null;
    }
}
