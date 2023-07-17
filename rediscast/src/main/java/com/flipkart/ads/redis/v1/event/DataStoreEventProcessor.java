package com.flipkart.ads.redis.v1.event;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.cache.RedisInMemoryMap;
import lombok.extern.slf4j.Slf4j;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
public class DataStoreEventProcessor {
    private static final String ENTRY_ADDED = "entryAdded";
    private static final String ENTRY_UPDATED = "entryUpdated";
    private static final String ENTRY_DELETED = "entryDeleted";
    private static final String CHANGE_PROPAGATION_DELAY = "changePropagationDelay";

    private final RedisInMemoryMap redisInMemoryMap;
    private final MetricRegistry metricRegistry;
    private final RedisDataStoreChangePropagator<String, Object> externalChangePropagator;
    private final Timer changePropagationDelay;
    private final String namespace;

    public DataStoreEventProcessor(RedisInMemoryMap redisInMemoryMap, RedisDataStoreChangePropagator<String, Object> externalChangePropagator,
                                   String namespace, MetricRegistry metricRegistry) {
        this.redisInMemoryMap = redisInMemoryMap;
        this.externalChangePropagator = externalChangePropagator;
        this.metricRegistry = metricRegistry;
        this.namespace = namespace;
        this.changePropagationDelay = metricRegistry.timer(name(DataStoreEventProcessor.class, this.namespace, CHANGE_PROPAGATION_DELAY));
    }

    public void processEntryAddedEvent(boolean initialised, RedisEntity<String, Object> event, boolean skipPublishMetrics) throws RedisDataStoreChangePropagatorException {
        try (Timer.Context context = getTimer(ENTRY_ADDED).time()) {
            if (initialised || isNewEvent(event)) {
                redisInMemoryMap.update(event);
                if (!skipPublishMetrics) {
                    changePropagationDelay.update(System.currentTimeMillis() - event.getEventTime(), MILLISECONDS);
                }
                if (externalChangePropagator != null) externalChangePropagator.entryAdded(event);
                log.debug("Processed entryAdded for namespace: {}, key: {}, timestamp: {}", event.getType(), event.getKey(), event.getEventTime());
            }
        }
    }

    public void processEntryUpdatedEvent(boolean initialised, RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        try (Timer.Context context = getTimer(ENTRY_UPDATED).time()) {
            if (initialised || isNewEvent(event)) {
                redisInMemoryMap.update(event);
                changePropagationDelay.update(System.currentTimeMillis() - event.getEventTime(), MILLISECONDS);
                if (externalChangePropagator != null) externalChangePropagator.entryUpdated(event);
                log.debug("Processed entryUpdated for namespace: {}, key: {}, timestamp: {}", event.getType(), event.getKey(), event.getEventTime());
            }
        }
    }

    public void processEntryDeletedEvent(boolean initialised, RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        try (Timer.Context context = getTimer(ENTRY_DELETED).time()) {
            if (initialised || isNewEvent(event)) {
                redisInMemoryMap.delete(event);
                changePropagationDelay.update(System.currentTimeMillis() - event.getEventTime(), MILLISECONDS);
                if (externalChangePropagator != null) externalChangePropagator.entryDeleted(event);
                log.debug("Processed entryDeleted for namespace: {}, key: {}, timestamp: {}", event.getType(), event.getKey(), event.getEventTime());
            }
        }
    }

    private boolean isNewEvent(RedisEntity<String, Object> redisEntity) {
        return redisEntity.getEventTime() >= redisInMemoryMap.getLastKnownTimestamp(redisEntity.getKey());
    }

    private Timer getTimer(String operation) {
        return metricRegistry.timer(name(DataStoreEventProcessor.class, this.namespace, operation));
    }
}
