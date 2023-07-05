package com.flipkart.ads.redis.v1.event;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreEventListenerException;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.cache.RedisInMemoryMap;
import com.flipkart.ads.redis.v1.config.processor.BootstrapConfig;
import com.flipkart.ads.redis.v1.locks.MapBasedMultiLock;
import com.flipkart.ads.redis.v1.locks.MultiLock;
import com.flipkart.ads.redis.v1.locks.StripedMultiLock;
import com.flipkart.ads.redis.v1.stream.RedisDataStoreCDC;
import com.google.common.collect.Iterables;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
public class RedisDataStoreEventProcessorImpl implements RedisDataStoreEventProcessor, RedisDataStoreChangePropagator<String, Object> {
    private static final String ENTRY_ADDED = "entryAdded";
    private static final String ENTRY_UPDATED = "entryUpdated";
    private static final String ENTRY_DELETED = "entryDeleted";
    private static final String CHANGE_PROPAGATION_DELAY = "changePropagationDelay";

    private final AtomicBoolean initialised = new AtomicBoolean(false);
    private final String mapName;
    private final MetricRegistry metricRegistry;
    private final RedisDataStoreCDC ingestionDataStoreReadOnlyClient;
    private final RedisDataStoreChangePropagator<String, Object> externalChangePropagator;
    private final MultiLock multiLock;
    private final RedisInMemoryMap versionMap;
    private final Timer changePropagationDelay;
    private final DataStoreEventProcessor dataStoreEventProcessor;
    private final Meter kvGetException;
    private final BootstrapConfig bootstrapConfig;
    private String listenerId;


    public RedisDataStoreEventProcessorImpl(RedisDataStoreCDC ingestionDataStoreReadOnlyClient,
                                            RedisDataStoreChangePropagator<String, Object> externalChangePropagator,
                                            String mapName, int stripes, MetricRegistry metricRegistry, BootstrapConfig bootstrapConfig) {

        this(ingestionDataStoreReadOnlyClient, externalChangePropagator, mapName, metricRegistry,
                new StripedMultiLock(stripes, false), bootstrapConfig);

    }


    public RedisDataStoreEventProcessorImpl(RedisDataStoreCDC ingestionDataStoreReadOnlyClient,
                                            RedisDataStoreChangePropagator<String, Object> externalChangePropagator,
                                            String mapName, MetricRegistry metricRegistry, BootstrapConfig bootstrapConfig) {

        this(ingestionDataStoreReadOnlyClient, externalChangePropagator, mapName, metricRegistry, new MapBasedMultiLock(), bootstrapConfig);
    }

    protected RedisDataStoreEventProcessorImpl(RedisDataStoreCDC ingestionDataStoreReadOnlyClient,
                                               RedisDataStoreChangePropagator<String, Object> externalChangePropagator,
                                               String mapName, MetricRegistry metricRegistry, MultiLock multiLock, BootstrapConfig bootstrapConfig) {
        this.mapName = mapName;
        this.metricRegistry = metricRegistry;
        this.ingestionDataStoreReadOnlyClient = ingestionDataStoreReadOnlyClient;
        this.externalChangePropagator = externalChangePropagator;
        this.changePropagationDelay = metricRegistry.timer(name(RedisDataStoreEventProcessorImpl.class, this.mapName, CHANGE_PROPAGATION_DELAY));
        this.versionMap = new RedisInMemoryMap(RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP, mapName);
        this.dataStoreEventProcessor = new DataStoreEventProcessor(this.versionMap, externalChangePropagator, mapName, metricRegistry);
        this.multiLock = multiLock;
        this.kvGetException = metricRegistry.meter(name(RedisDataStoreEventProcessorImpl.class, mapName, "kvGetException"));
        this.bootstrapConfig = bootstrapConfig;
    }

    @Override
    public void init() throws Exception {
        initialised.compareAndSet(true, false);
        // @todo: handle on the fly re-initialisation by stopping the listeners.
        try (Timer.Context ignored = getEventProcessorTimer(mapName, "init").time()) {
            listenerId = ingestionDataStoreReadOnlyClient.addListener(mapName, this);
            if (bootstrapConfig != null) {
                incrementalFullScan();
            } else {
                fullScan();
            }
        } catch (RedisDataStoreException | RedisDataStoreChangePropagatorException e) {
            log.error("RedisDataStoreException in init of cache", e);
            throw new RedisDataStoreEventListenerException("Initialization error could not get whole map", e);
        }
        initialised.compareAndSet(false, true);
        versionMap.clear();
    }

    private void fullScan() throws RedisDataStoreException {
        try (Timer.Context ignored = getEventProcessorTimer(this.mapName, "fullScan").time()) {
            log.info("Starting single shot full scan of mapName {} ", mapName);
            Set<String> keySet = ingestionDataStoreReadOnlyClient.keySet(mapName);
            for (List<String> keys : Iterables.partition(keySet, bootstrapConfig.getBatchSize())) {
                Map<String, RedisEntity<String, Object>> storeData = new HashMap<>();
                int retry = 3;
                while (retry > 0) {
                    try {
                        storeData.putAll(ingestionDataStoreReadOnlyClient.getBatch(mapName, new LinkedHashSet<>(keys)));
                        retry = 0;
                    } catch (RedisDataStoreException e) {
                        log.error("Error while fetching batch data {}", this.mapName, e);
                        retry--;
                    }
                }
                storeData.entrySet().parallelStream().forEach(this::processKVStoreEvent);
            }
            log.info("Processed all data for mapName : {}, count : {} ", mapName, keySet.size());
        }
    }

    private void incrementalFullScan() throws RedisDataStoreException {
        try (Timer.Context ignored = getEventProcessorTimer(this.mapName, "incrementalFullScan").time()) {
            log.error("Starting Redis Event Processor incremental full scan of mapName {} ", mapName);
            ExecutorService executorService = new ThreadPoolExecutor(bootstrapConfig.getCorePoolSize(), bootstrapConfig.getMaxPoolSize(), 10000L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
            Set<String> keys = ingestionDataStoreReadOnlyClient.keySet(mapName);
            if (CollectionUtils.isNotEmpty(keys)) {
                final List<Future<?>> futures = submitScanTasks(executorService, keys);
                for (Future<?> future : futures) future.get();
            } else {
                log.error("No data in incremental full scan of mapName {} ", mapName);
            }
            executorService.shutdown();
        } catch (Throwable e) {
            throw new RedisDataStoreException("Thread Execution/Interrupted Exception : " + e.getMessage());
        }
    }

    private List<Future<?>> submitScanTasks(ExecutorService executorService, Set<String> keySet) throws RedisDataStoreException {
        try {
            List<Future<?>> futures = new ArrayList<>();
            int batch = 0;
            for (List<String> keys : Iterables.partition(keySet, bootstrapConfig.getBatchSize())) {
                futures.add(executorService.submit(new RedisDataStoreBatchProcessorTask(dataStoreEventProcessor, multiLock, mapName, ingestionDataStoreReadOnlyClient, new LinkedHashSet<>(keys), ++batch, metricRegistry)));
            }
            return futures;
        } catch (Exception e) {
            throw new RedisDataStoreException("Namespace not registered : " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isInitialised() {
        return initialised.get();
    }

    @Override
    public void entryAdded(RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        String key = event.getKey();
        try {
            multiLock.lock(key);
            Object value = event.getValue();
            if (value == null) {
                value = getValueFromKVStore(key, event.getEventTime());
            }
            if (nonNull(value)) {
                event.setValue(value);
                if (isInitialised()) {
                    try (Timer.Context ignored = getChangePropagatorTimer(mapName, ENTRY_ADDED).time()) {
                        changePropagationDelay.update(System.currentTimeMillis() - event.getEventTime(), MILLISECONDS);
                        if (externalChangePropagator != null) externalChangePropagator.entryAdded(event);
                        log.debug("Processed entryAdded event: {} ", event.getType());
                    }
                } else {
                    this.dataStoreEventProcessor.processEntryAddedEvent(isInitialised(), event, false);
                }
            } else {
                log.info("Received entryAdded but value is not present in Redis id: {} ", key);
            }
        } finally {
            multiLock.unlock(key);
        }
    }

    @Override
    public void entryUpdated(RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        String key = event.getKey();
        try {
            multiLock.lock(key);
            Object value = event.getValue();
            if (value == null) {
                value = getValueFromKVStore(key, event.getEventTime());
            }
            if (nonNull(value)) {
                event.setValue(value);
                if (isInitialised()) {
                    try (Timer.Context ignored = getChangePropagatorTimer(mapName, ENTRY_UPDATED).time()) {
                        changePropagationDelay.update(System.currentTimeMillis() - event.getEventTime(), MILLISECONDS);
                        if (externalChangePropagator != null) externalChangePropagator.entryUpdated(event);
                        log.debug("Processed entryUpdated event: {} ", event.getType());
                    }
                } else {
                    this.dataStoreEventProcessor.processEntryUpdatedEvent(isInitialised(), event);
                }
            } else {
                log.info("Received entryUpdated but value is not present in Redis : {}", key);
            }
        } finally {
            multiLock.unlock(key);
        }
    }

    @Override
    public void entryDeleted(RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        String key = event.getKey();
        try {
            multiLock.lock(key);
            Object value = event.getValue();
            if (isNull(value)) {
                value = getValueFromKVStore(key, -1);
            }
            if (nonNull(value)) {
                event.setValue(value);
                if (isInitialised()) {
                    try (Timer.Context ignored = getChangePropagatorTimer(mapName, ENTRY_DELETED).time()) {
                        changePropagationDelay.update(System.currentTimeMillis() - event.getEventTime(), MILLISECONDS);
                        if (externalChangePropagator != null) externalChangePropagator.entryDeleted(event);
                        log.debug("Processed entryDeleted event: {} ", event.getType());
                    }
                } else {
                    this.dataStoreEventProcessor.processEntryDeletedEvent(isInitialised(), event);
                }
            } else {
                log.info("Received entryDeleted but value is not present in Redis : {} {}", key, event);
            }
        } finally {
            multiLock.unlock(key);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            ingestionDataStoreReadOnlyClient.removeListener(listenerId);
        } catch (RedisDataStoreChangePropagatorException e) {
            log.error("Error while stopping listener for mapName: {} ", mapName, e);
            throw new IOException("Error while stopping listener for mapName:" + mapName, e);
        }
    }

    private void processKVStoreEvent(Map.Entry<String, RedisEntity<String, Object>> entry) {
        try {
            entry.getValue().setMapName(mapName);
            processFullScanEntryAddedEvent(entry.getValue());
        } catch (RedisDataStoreChangePropagatorException e) {
            log.error("Error while updating data: ", e);
        }
    }

    private Object getValueFromKVStore(String key, long eventTime) throws RedisDataStoreChangePropagatorException {
        try (Timer.Context ignored = getEventProcessorTimer(mapName, "kvGet").time()) {
            RedisEntity<String, Object> redisEntity = ingestionDataStoreReadOnlyClient.get(mapName, key, eventTime);
            return redisEntity != null ? redisEntity.getValue() : null;
        } catch (RedisDataStoreException e) {
            kvGetException.mark();
            log.error("Failed to fetch value from Redis for key: {}", key, e);
            throw new RedisDataStoreChangePropagatorException(e.getMessage(), e);
        }
    }

    private Timer getEventProcessorTimer(String mapName, String operation) {
        return metricRegistry.timer(name(RedisDataStoreEventProcessorImpl.class, mapName, operation));
    }

    private Timer getChangePropagatorTimer(String mapName, String operation) {
        return metricRegistry.timer(name(RedisDataStoreEventProcessorImpl.class, mapName, operation));
    }

    private void processFullScanEntryAddedEvent(RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        Object key = event.getKey();
        try {
            multiLock.lock(key);
            this.dataStoreEventProcessor.processEntryAddedEvent(isInitialised(), event, true);
        } finally {
            multiLock.unlock(key);
        }
    }
}
