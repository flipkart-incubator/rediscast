package com.flipkart.ads.redis.v1.event;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.locks.MultiLock;
import com.flipkart.ads.redis.v1.stream.RedisDataStoreCDC;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.codahale.metrics.MetricRegistry.name;

@Slf4j
public class RedisDataStoreBatchProcessorTask extends Thread {
    private final DataStoreEventProcessor dataStoreEventProcessor;
    private final MultiLock multiLock;
    private final String mapName;
    private final RedisDataStoreCDC readOnlyClient;
    private final Set<String> keyset;
    private final int batch;
    private final Timer processBatchTimer;


    public RedisDataStoreBatchProcessorTask(DataStoreEventProcessor dataStoreEventProcessor, MultiLock multiLock, String mapName, RedisDataStoreCDC readOnlyClient, Set<String> keyset, int batch, MetricRegistry metricRegistry) {
        this.dataStoreEventProcessor = dataStoreEventProcessor;
        this.multiLock = multiLock;
        this.mapName = mapName;
        this.readOnlyClient = readOnlyClient;
        this.keyset = keyset;
        this.batch = batch;
        this.processBatchTimer = metricRegistry.timer(name(RedisDataStoreBatchProcessorTask.class, mapName, "bootstrapBatch"));
    }

    @Override
    public void run() {
        log.info("Started scanning data for namespace {} - batch {} ", mapName, batch);
        try (Timer.Context ignored = processBatchTimer.time()) {
            int retry = 3;
            final Map<String, RedisEntity<String, Object>> batchWithMeta = new HashMap<>();
            while (retry > 0) {
                try {
                    batchWithMeta.putAll(readOnlyClient.getBatch(mapName, keyset));
                    retry = 0;
                } catch (RedisDataStoreException e) {
                    log.error("Error while fetching batch data {}", this.mapName, e);
                    retry--;
                }
            }
            if (batchWithMeta.size() == 0) {
                throw new RedisDataStoreException("Error while fetching batch data");
            }
            keyset.parallelStream().forEach(k -> processKVStoreEvent(k, batchWithMeta.getOrDefault(k, null)));
        } catch (RedisDataStoreException e) {
            log.error("Unable to get bootstrap data from KVDataStore");
            throw new RuntimeException("Unable to bootstrap data from KVDataStore", e);
        }
    }

    private void processKVStoreEvent(String key, RedisEntity<String, Object> entry) {
        try {
            if (entry == null) {
                entry = new RedisEntity<>(mapName, key, System.currentTimeMillis());
            }
            entry.setMapName(mapName);
            if (entry.getValue() != null) {
                processFullScanEntryAddedEvent(entry);
            } else {
                processFullScanEntryDeletedEvent(entry);
            }
        } catch (RedisDataStoreChangePropagatorException e) {
            log.error("Error while updating data: ", e);
        } catch (Exception e) {
            log.error("Deserialization issue while processing event {}", e.getMessage());
        }
    }

    private void processFullScanEntryAddedEvent(RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        Object key = event.getKey();
        try {
            multiLock.lock(key);
            this.dataStoreEventProcessor.processEntryAddedEvent(false, event, true);
        } finally {
            multiLock.unlock(key);
        }
    }

    private void processFullScanEntryDeletedEvent(RedisEntity<String, Object> event) throws RedisDataStoreChangePropagatorException {
        Object key = event.getKey();
        try {
            multiLock.lock(key);
            this.dataStoreEventProcessor.processEntryDeletedEvent(false, event);
        } finally {
            multiLock.unlock(key);
        }
    }
}
