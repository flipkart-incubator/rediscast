package com.flipkart.ads.redis.v1.client;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.exceptions.RedisDataTransformerException;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.flipkart.ads.redis.v1.transformers.RedisTransformer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractRedisClient {
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    protected static final String ENTITY_KEY = "entityKey";
    protected static final String ENTITY_TYPE = "entityType";
    protected static final String ENTITY_VALUE = "entityValue";
    protected static final String COMMAND = "command";
    protected static final String EVENT_TIME = "eventTime";
    protected static final String UNDER_SCORE = "_";
    protected static final String IDS_POSTFIX = "_ids";

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("IST"));
        OBJECT_MAPPER
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID)
                .enable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
                .setDateFormat(dateFormat);
    }

    protected final MetricRegistry metricRegistry;
    protected final String idsPostFix = "ids";
    protected final Map<String, AbstractRedisClient.EntityStreamConfigs> mapNameToEntityStreamConfig;
    protected final Map<String, RedisTransformer> mapNameToEntityDataTransformers;
    private final Pool<Jedis> jedisPool;

    public AbstractRedisClient(Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> mapNameToEntityStreamConfig,
                               Map<RedisMap, RedisTransformer> mapNameToEntityDataTransformers,
                               Pool<Jedis> jedisPool, MetricRegistry metricRegistry) {
        this.mapNameToEntityStreamConfig = mapNameToEntityStreamConfig.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().getMapName(), Map.Entry::getValue));
        this.mapNameToEntityDataTransformers = mapNameToEntityDataTransformers.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().getMapName(), Map.Entry::getValue));
        this.jedisPool = jedisPool;
        this.metricRegistry = metricRegistry;
    }

    protected Object execRedisCommand(String command) {
        try (Jedis resource = jedisPool.getResource()) {
            return resource.eval(command);
        } catch (Exception exp) {
            log.error("Error in executing the given script: {}", exp.getMessage());
            throw exp;
        }
    }

    // TODO: this is not related to redis stream library. can be moved to application logic
    public Map<String, String> getHashAll(String key) throws RedisDataStoreException {
        try (Jedis resource = jedisPool.getResource()) {
            Map<String, String> result = resource.hgetAll(key);
            if (MapUtils.isEmpty(result)) {
                return new HashMap<>();
            }
            return result;
        } catch (Exception e) {
            log.error("Exception in getHashAll : hashName: {}, e: {}", key, e);
            markExceptionMeter(this.getClass(), "getHashAll", key);
            throw new RedisDataStoreException("Could not fetch data for Hash: " + key, e);
        }
    }

    protected Object deserializeEntity(String type, String value) {
        Map.Entry<String, RedisEntity<String, Object>> entry = null;
        try {
            entry = getEntityData(type, value);
        } catch (IOException | RedisDataTransformerException e) {
            log.error("Error while deserializing the Entry {} {}", type, value);
        }
        return entry != null ? entry.getValue().getValue() : null;
    }

    protected long getStreamMaxLength(String type) {
        AbstractRedisClient.EntityStreamConfigs streamConfig = mapNameToEntityStreamConfig.get(type);
        if (streamConfig != null) {
            return streamConfig.getStreamMaxLength();
        }
        return 1000L;
    }

    protected Map.Entry<String, RedisEntity<String, Object>> getEntityData(String mapName, String result) throws IOException, RedisDataTransformerException {
        try {
            validateIfDataTransformerExistsForTheMap(mapName);
            JavaType type = OBJECT_MAPPER.getTypeFactory().constructParametricType(RedisEntity.class, String.class, String.class);
            RedisEntity<String, String> entity = OBJECT_MAPPER.readValue(result, type);
            Object transformedValue = mapNameToEntityDataTransformers.get(mapName).revert(entity.getValue());
            RedisEntity<String, Object> transformedEntity = new RedisEntity<>(entity.getType(), entity.getKey(), entity.getEventTime());
            transformedEntity.setValue(transformedValue);
            transformedEntity.setTtl(entity.getTtl());
            transformedEntity.setMapName(entity.getMapName());
            return new AbstractMap.SimpleImmutableEntry<>(entity.getKey(), transformedEntity);
        } catch (Exception e) {
            log.error("Error in deserializing result into corresponding HazelcastMap entity for mapName:{} result:{} ", mapName, result, e);
            throw e;
        }
    }

    protected boolean keyExists(Pool<Jedis> jedis, String key) {
        try (Jedis resource = jedis.getResource()) {
            return resource.exists(key);
        }
    }

    protected String getRedisKey(String mapName, String key) {
        return mapName + "_" + key;
    }

    protected void markExceptionMeter(Class cl, String operation, String mapName) {
        metricRegistry.meter(MetricRegistry.name(cl, mapName, operation, "exception")).mark();
    }

    protected void validateIfDataTransformerExistsForTheMap(String mapName) throws RedisDataTransformerException {
        if (mapNameToEntityDataTransformers.get(mapName) != null) {
            return;
        }

        throw new RedisDataTransformerException("Invalid or no data transformer found for the given map: " + mapName);
    }

    @Getter
    @AllArgsConstructor
    public static class EntityStreamConfigs {
        final String name;
        final int count;
        final long block;
        final long streamMaxLength;
    }
}
