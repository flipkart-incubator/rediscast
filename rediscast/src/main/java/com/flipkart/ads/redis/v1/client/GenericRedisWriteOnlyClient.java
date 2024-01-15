package com.flipkart.ads.redis.v1.client;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.exceptions.RedisWriteSynchronizationException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.pool.RedisSynchronizationConfig;
import com.flipkart.ads.redis.v1.exceptions.RedisDataTransformerException;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.flipkart.ads.redis.v1.transformers.RedisTransformer;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Pool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.flipkart.ads.redis.v1.model.StreamEvent.StreamEventType.DELETE;
import static com.flipkart.ads.redis.v1.model.StreamEvent.StreamEventType.UPDATE;
import static redis.clients.jedis.StreamEntryID.NEW_ENTRY;


@Slf4j
public class GenericRedisWriteOnlyClient extends AbstractRedisClient implements RedisWriteOnlyClient {
    private final Pool<Jedis> jedis;
    private final Timer replicationTimer;
    private final Meter replicationFailure;

    public GenericRedisWriteOnlyClient(Pool<Jedis> jedis,
                                       Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> mapNameToEntityStreamConfig,
                                       Map<RedisMap, RedisTransformer> mapNameToEntityDataConfig,
                                       MetricRegistry metricRegistry) {
        super(mapNameToEntityStreamConfig, mapNameToEntityDataConfig, jedis, metricRegistry);
        this.jedis = jedis;
        this.replicationTimer = metricRegistry.timer(MetricRegistry.name(GenericRedisWriteOnlyClient.class, "redis_replication_timer"));
        this.replicationFailure = metricRegistry.meter(MetricRegistry.name(GenericRedisWriteOnlyClient.class, "redis_replication_failure"));
    }

    /**
     * Writes to Redis in a synchronous manner.
     * Every write to master is explicitly replicated to a number of slave as configured in redisSynchronizationConfig
     *
     * @param entity                     data to write in Redis
     * @param finallyCallBack            callback function to execute
     * @param redisSynchronizationConfig config related to making redis write synchronous
     * @throws RedisWriteSynchronizationException
     */
    @Override
    public void updateInRedisSynchronously(RedisEntity<String, Object> entity, Consumer<String> finallyCallBack, RedisSynchronizationConfig redisSynchronizationConfig) throws RedisWriteSynchronizationException {
        Timer.Context replicationTimer = null;
        try {
            updateEntityInRedis(entity, finallyCallBack);
            replicationTimer = this.replicationTimer.time();
            OptionalInt isReplicated = IntStream.range(0, redisSynchronizationConfig.getRetries())
                    .filter(i -> replicate(redisSynchronizationConfig.getReplicas(), redisSynchronizationConfig.getWaitTimeout()) >= redisSynchronizationConfig.getReplicas()).findFirst();
            if (!isReplicated.isPresent()) {
                replicationFailure.mark();
                log.error("[ERROR]: No. of slaves where replication happened is < " + redisSynchronizationConfig.getReplicas());
                throw new RedisWriteSynchronizationException("Redis Synchronous Update Failed for " + entity.getKey());
            }
        } catch (Exception e) {
            log.error("Entity : {}, id : {}, exception : {}", entity.getType(), entity.getKey(), e);
            throw new RedisWriteSynchronizationException(e.getMessage(), e);
        } finally {
            if (replicationTimer != null)
                replicationTimer.stop();
        }
    }

    /**
     * Delete from Redis in a synchronous manner.
     * Every delete to master is explicitly replicated to a number of slave as configured in redisSynchronizationConfig
     *
     * @param entity                     data to remove from Redis
     * @param finallyCallBack            callback function to execute
     * @param redisSynchronizationConfig config related to making redis remove synchronous
     * @throws RedisWriteSynchronizationException
     */

    @Override
    public void removeFromRedisSynchronously(RedisEntity<String, Object> entity, Consumer<String> finallyCallBack, RedisSynchronizationConfig redisSynchronizationConfig) throws RedisWriteSynchronizationException {
        Timer.Context replicationTimer = null;
        try {
            removeEntityFromRedis(entity, finallyCallBack);
            replicationTimer = this.replicationTimer.time();
            OptionalInt isReplicated = IntStream.range(0, redisSynchronizationConfig.getRetries())
                    .filter(i -> replicate(redisSynchronizationConfig.getReplicas(), redisSynchronizationConfig.getWaitTimeout()) >= redisSynchronizationConfig.getReplicas()).findFirst();
            if (!isReplicated.isPresent()) {
                replicationFailure.mark();
                throw new RedisWriteSynchronizationException("Redis Synchronous Remove Failed for " + entity.getKey());
            }
        } catch (Exception e) {
            log.error("Entity : {}, id : {}, exception : {}", entity.getType(), entity.getKey(), e);
            throw new RedisWriteSynchronizationException(e.getMessage(), e);
        } finally {
            if (replicationTimer != null)
                replicationTimer.stop();
        }
    }

    @Timed
    @ExceptionMetered
    private void updateEntityInRedis(RedisEntity<String, Object> entity, Consumer<String> finallyCallBack) throws RedisDataTransformerException, JsonProcessingException {
        try (Jedis resource = jedis.getResource()) {
            // Check if respective data transformer is present
            validateIfDataTransformerExistsForTheMap(entity.getType());

            Map<String, String> values = new HashMap<>();
            values.put(ENTITY_KEY, entity.getKey());
            values.put(ENTITY_TYPE, entity.getType());
            values.put(COMMAND, UPDATE.toString());
            values.put(EVENT_TIME, String.valueOf(entity.getEventTime()));
            String value = mapNameToEntityDataTransformers.get(entity.getType()).apply(entity.getValue());
            RedisEntity<String, Object> copiedEntity = entity.getShallowCopy();
            copiedEntity.setValue(value);
            Transaction tx = resource.multi();
            tx.set(entity.getType() + UNDER_SCORE + entity.getKey(), OBJECT_MAPPER.writeValueAsString(copiedEntity));
            tx.sadd(entity.getType() + IDS_POSTFIX, entity.getKey());
            tx.xadd(entity.getType(), NEW_ENTRY, values, getStreamMaxLength(entity.getType()), false);
            tx.persist(entity.getType() + UNDER_SCORE + entity.getKey());
            List<Object> formattedResponse = tx.exec();
            for (Object o : formattedResponse) {
                if (o.getClass().equals(JedisDataException.class)) {
                    log.error("Entity : {}, id : {}, Jedis Exception : {}", entity.getType(), entity.getKey(), o);
                    markExceptionMeter(this.getClass(), "updateFromRedisJedisException", entity.getType());
                    throw new JedisDataException("Update From Redis Jedis Exception", (JedisDataException) o);
                }
            }
        } catch (Exception e) {
            log.error("Entity : {}, id : {}, exception : {}", entity.getType(), entity.getKey(), e);
            markExceptionMeter(this.getClass(), "updateEntityInRedis", entity.getType());
            throw e;
        } finally {
            if (finallyCallBack != null)
                finallyCallBack.accept(entity.getType() + entity.getKey());
        }
    }

    @Timed
    @ExceptionMetered
    public void removeEntityFromRedis(RedisEntity<String, Object> entity, Consumer<String> finallyCallBack) {
        try (Jedis resource = jedis.getResource()) {
            Map<String, String> values = new HashMap<>();
            values.put(ENTITY_KEY, entity.getKey());
            values.put(ENTITY_TYPE, entity.getType());
            values.put(COMMAND, DELETE.toString());
            values.put(EVENT_TIME, String.valueOf(entity.getEventTime()));
            Transaction tx = resource.multi();
            tx.srem(entity.getType() + IDS_POSTFIX, entity.getKey());
            tx.xadd(entity.getType(), NEW_ENTRY, values, getStreamMaxLength(entity.getType()), false);
            tx.expire(entity.getType() + UNDER_SCORE + entity.getKey(), 600);
            List<Object> formattedResponse = tx.exec();
            for (Object o : formattedResponse) {
                if (o.getClass().equals(JedisDataException.class)) {
                    log.error("Entity : {}, id : {}, Jedis Exception : {}", entity.getType(), entity.getKey(), o);
                    markExceptionMeter(this.getClass(), "removeFromRedisJedisException", entity.getType());
                    throw new JedisDataException("Remove from Redis JedisEx exception", (JedisDataException) o);
                }
            }
        } catch (Exception e) {
            log.error("Entity : {}, id : {}, exception : {}", entity.getType(), entity.getKey(), e);
            markExceptionMeter(this.getClass(), "removeEntityFromRedis", entity.getType());
            throw e;
        } finally {
            if (finallyCallBack != null)
                finallyCallBack.accept(entity.getType() + entity.getKey());
        }
    }

    @Timed
    @ExceptionMetered
    @Override
    public boolean containsKey(String mapName, String key) throws RedisDataStoreException {
        return keyExists(jedis, getRedisKey(mapName, key));
    }

    public long size(String mapName) {
        try (Jedis resource = jedis.getResource()) {
            return resource.scard(getRedisKey(mapName, idsPostFix));
        } catch (Exception e) {
            log.error("Error while checking size of keys in a map {}", mapName, e);
            markExceptionMeter(this.getClass(), "size", mapName);
        }
        return 0;
    }

    @Timed
    @ExceptionMetered
    @Override
    public boolean containsKey(String key) throws RedisDataStoreException {
        return keyExists(jedis, key);
    }

    private long replicate(int replicas, long timeout) {
        try (Jedis resource = jedis.getResource()) {
            return resource.waitReplicas(replicas, timeout);
        } catch (Exception e) {
            return 0;
        }
    }
}
