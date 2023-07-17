package com.flipkart.ads.redis.v1.cache;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.exceptions.RedisInMemoryCacheException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.config.processor.BootstrapConfig;
import com.flipkart.ads.redis.v1.event.DataStoreEventProcessor;
import com.flipkart.ads.redis.v1.event.RedisDataStoreBatchProcessorTask;
import com.flipkart.ads.redis.v1.locks.MapBasedMultiLock;
import com.flipkart.ads.redis.v1.locks.MultiLock;
import com.flipkart.ads.redis.v1.locks.StripedMultiLock;
import com.flipkart.ads.redis.v1.stream.RedisDataStoreCDC;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Slf4j
public class RedisDataInMemoryCacheImpl implements RedisDataInMemoryCache, RedisDataStoreChangePropagator<String, Object>, TTLExpiryListener<String, Object> {
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final RedisInMemoryMap redisInMemoryMap;
    private final String mapName;
    private final MetricRegistry metricRegistry;
    private final RedisDataStoreCDC redisDataStoreCDC;
    private final DataStoreEventProcessor dataStoreEventProcessor;
    private final MultiLock multiLock;
    private final Meter kvGetException;
    private final BootstrapConfig bootstrapConfig;
    private String listenerId;

    public RedisDataInMemoryCacheImpl(String mapName, RedisInMemoryMap redisInMemoryMap,
                                      RedisDataStoreCDC redisDataStoreCDC,
                                      RedisDataStoreChangePropagator<String, Object> changePropagator,
                                      int stripes, BootstrapConfig bootstrapConfig,
                                      MetricRegistry metricRegistry) {

        this(mapName, redisInMemoryMap, redisDataStoreCDC, changePropagator, new StripedMultiLock(stripes, false), bootstrapConfig, metricRegistry);
    }

    public RedisDataInMemoryCacheImpl(String mapName, RedisInMemoryMap redisInMemoryMap,
                                      RedisDataStoreCDC redisDataStoreCDC,
                                      RedisDataStoreChangePropagator<String, Object> changePropagator,
                                      BootstrapConfig bootstrapConfig, MetricRegistry metricRegistry) {
        this(mapName, redisInMemoryMap, redisDataStoreCDC, changePropagator, new MapBasedMultiLock(), bootstrapConfig, metricRegistry);
    }

    protected RedisDataInMemoryCacheImpl(String mapName,
                                         RedisInMemoryMap redisInMemoryMap,
                                         RedisDataStoreCDC redisDataStoreCDC,
                                         RedisDataStoreChangePropagator<String, Object> changePropagator,
                                         MultiLock multiLock,
                                         BootstrapConfig bootstrapConfig,
                                         MetricRegistry metricRegistry) {

        this.metricRegistry = metricRegistry;
        this.mapName = mapName;
        this.redisInMemoryMap = redisInMemoryMap;
        this.redisDataStoreCDC = redisDataStoreCDC;
        if (redisInMemoryMap.getRedisLocalMapType().equals(RedisInMemoryMap.RedisLocalMapType.LOCAL_DATA_WITH_TTL_SUPPORTED)) {
            redisInMemoryMap.setTTLExpiryListener(this);
        }
        this.multiLock = multiLock;
        this.bootstrapConfig = bootstrapConfig;
        this.dataStoreEventProcessor = new DataStoreEventProcessor(redisInMemoryMap, changePropagator, mapName, metricRegistry);
        this.kvGetException = metricRegistry.meter(name(RedisDataInMemoryCacheImpl.class, mapName, "kvGetException"));
    }


    @Override
    public void init() throws RedisInMemoryCacheException {
        initialized.compareAndSet(true, false);
        // @todo: handle on the fly re-initialisation by stopping the listeners.
        try (Timer.Context context = getEventProcessorTimer(mapName, "init").time()) {
            listenerId = redisDataStoreCDC.addListener(mapName, this);
            if (bootstrapConfig != null) {
                incrementalFullScan();
            } else {
                fullScan();
            }
        } catch (RedisDataStoreException e) {
            log.error("RedisDataStoreException in init of cache", e);
            throw new RedisInMemoryCacheException("Initialization error could not get whole map", e);
        } catch (RedisDataStoreChangePropagatorException e) {
            log.error("RedisDataStoreChangePropagatorException in init of cache", e);
            throw new RedisInMemoryCacheException("Initialization error could attach listener map", e);
        }
        initialized.compareAndSet(false, true);
        if (redisInMemoryMap.getRedisLocalMapType().equals(RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP))
            redisInMemoryMap.clear(); //If indexer master switch happens and old data was not cleared then this logic was required.
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
            RedisEntity<String, Object> redisEntity = redisDataStoreCDC.get(mapName, key, eventTime);
            return redisEntity != null ? redisEntity.getValue() : null;
        } catch (RedisDataStoreException e) {
            kvGetException.mark();
            log.error("Failed to fetch value from Redis for key: {}", key, e);
            throw new RedisDataStoreChangePropagatorException(e.getMessage(), e);
        }
    }

    private Timer getEventProcessorTimer(String mapName, String operation) {
        return metricRegistry.timer(name(RedisDataInMemoryCacheImpl.class, mapName, operation));
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
                this.dataStoreEventProcessor.processEntryAddedEvent(isInitialized(), event, false);
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
                this.dataStoreEventProcessor.processEntryUpdatedEvent(isInitialized(), event);
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
                this.dataStoreEventProcessor.processEntryDeletedEvent(isInitialized(), event);
            } else {
                log.info("Received entryDeleted but value is not present in Redis : {} {}", key, event);
            }
        } finally {
            multiLock.unlock(key);
        }
    }

    private void fullScan() throws RedisDataStoreException {
        try (Timer.Context context = getEventProcessorTimer(this.mapName, "fullScan").time()) {
            log.info("Starting single shot full scan of mapName {} ", mapName);
            Set<String> keySet = redisDataStoreCDC.keySet(mapName);
            for (List<String> keys : Iterables.partition(keySet, bootstrapConfig.getBatchSize())) {
                Map<String, RedisEntity<String, Object>> storeData = new HashMap<>();
                int retry = 3;
                while (retry > 0) {
                    try {
                        storeData.putAll(redisDataStoreCDC.getBatch(mapName, new LinkedHashSet<>(keys)));
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
        try (Timer.Context context = getEventProcessorTimer(this.mapName, "incrementalFullScan").time()) {
            log.error("Starting Redis InMemory Cache incremental full scan of mapName {} ", mapName);
            ExecutorService executorService = new ThreadPoolExecutor(bootstrapConfig.getCorePoolSize(), bootstrapConfig.getMaxPoolSize(), 10000L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
            Set<String> keys = redisDataStoreCDC.keySet(mapName);
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
                futures.add(executorService.submit(new RedisDataStoreBatchProcessorTask(dataStoreEventProcessor, multiLock, mapName, redisDataStoreCDC, new LinkedHashSet<>(keys), ++batch, metricRegistry)));
            }
            return futures;
        } catch (Exception e) {
            throw new RedisDataStoreException("Namespace not registered : " + e.getMessage(), e);
        }
    }

    @Override
    public void ttlExpired(String key, Object value) {
        RedisEntity<String, Object> redisEntity = new RedisEntity<>(mapName, key, System.currentTimeMillis());
        redisEntity.setValue(value);
        redisEntity.setTtl(-1L);
        try {
            entryDeleted(redisEntity);
        } catch (RedisDataStoreChangePropagatorException e) {
            log.error("ttlExpired event notification for : {} failed. ", key, e);
            metricRegistry.meter(MetricRegistry.name(RedisDataInMemoryCacheImpl.class, mapName, "ttlExpiredException")).mark();
        }
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    @Override
    public boolean isInitialized() {
        return initialized.get();
    }

    @Override
    public <K, V> V get(K key) throws RedisInMemoryCacheException {
        return (V) redisInMemoryMap.get(key);
    }

    @Override
    public <K, V> Map<K, V> mGet(List<K> keys) throws RedisInMemoryCacheException {
        Map<K, V> resultMap = new HashMap<>();
        for (K key : keys) {
            resultMap.put(key, get(key));
        }
        return resultMap;
    }

    @Override
    public <V> Collection<V> values() throws RedisInMemoryCacheException {
        return (Collection<V>) redisInMemoryMap.values();
    }

    @Override
    public long size() throws RedisInMemoryCacheException {
        return redisInMemoryMap.size();
    }

    @Override
    public <K, V> Set<Map.Entry<K, V>> entrySet() throws RedisInMemoryCacheException {
        Set<Map.Entry<Object, RedisInMemoryMap.ValueWrapper>> wrappedEntries = redisInMemoryMap.entrySet();
        Set<Map.Entry<K, V>> entries = Sets.newHashSet();

        wrappedEntries.forEach(entry -> {
            entries.add(Maps.immutableEntry((K) entry.getKey(), (V) entry.getValue().getEntity()));
        });

        return entries;
    }

    @Override
    public void close() throws IOException {
        try {
            redisDataStoreCDC.removeListener(listenerId);
        } catch (RedisDataStoreChangePropagatorException e) {
            log.error("Error while stopping listener for mapName: {} ", mapName, e);
            throw new IOException("Error while stopping listener for mapName:" + mapName, e);
        }
    }

    private void processFullScanEntryAddedEvent(RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        Object key = event.getKey();
        try {
            multiLock.lock(key);
            this.dataStoreEventProcessor.processEntryAddedEvent(isInitialized(), event, true);
        } finally {
            multiLock.unlock(key);
        }
    }
}
