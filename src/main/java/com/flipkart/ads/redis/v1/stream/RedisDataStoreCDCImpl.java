package com.flipkart.ads.redis.v1.stream;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.ads.redis.v1.client.RedisReadOnlyClient;
import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Singleton
public class RedisDataStoreCDCImpl implements RedisDataStoreCDC {
    private static final int DEFAULT_LISTENER_INITIAL_DELAY = 50;
    private static final int DEFAULT_LISTENER_INVOKE_FREQ_MS = 1000;


    private final ScheduledExecutorService executorService;
    private final Map<String, ScheduledFuture<?>> runningTasks = new ConcurrentHashMap<>();
    private final MetricRegistry metricRegistry;
    private final RedisReadOnlyClient redisReadOnlyClient;

    @Inject
    public RedisDataStoreCDCImpl(MetricRegistry metricRegistry,
                                 RedisReadOnlyClient redisReadOnlyClient,
                                 @Named("redisCacheScheduler") ScheduledExecutorService executorService) {
        this.metricRegistry = metricRegistry;
        this.redisReadOnlyClient = redisReadOnlyClient;
        this.executorService = executorService;
    }

    @Override
    public String addListener(String mapName, RedisDataStoreChangePropagator<String, Object> changePropagator) throws RedisDataStoreChangePropagatorException {
        return addListener(mapName, changePropagator, DEFAULT_LISTENER_INITIAL_DELAY, DEFAULT_LISTENER_INVOKE_FREQ_MS);
    }

    @Override
    public String addListener(String mapName, RedisDataStoreChangePropagator<String, Object> changePropagator, int initialDelayMs, int frequencyMs) throws RedisDataStoreChangePropagatorException {
        try {
            validateAlreadyAttachedListener(mapName);
            log.error("Attaching listener for map name:{}", mapName);
            RedisStreamListener changeListener = new RedisStreamListenerImpl(metricRegistry, redisReadOnlyClient, mapName, true);
            RedisStreamListenerTask changeListenerTask = new RedisStreamListenerTask(metricRegistry, mapName, changeListener, changePropagator);
            ScheduledFuture<?> scheduledFuture = executorService.scheduleWithFixedDelay(changeListenerTask, initialDelayMs,
                    frequencyMs, TimeUnit.MILLISECONDS);
            runningTasks.put(changeListener.getId(), scheduledFuture);
            return changeListener.getId();
        } catch (Exception e) {
            log.error("Exception while adding listener mapName :{}", mapName, e);
            throw new RedisDataStoreChangePropagatorException("Could not attach listener for mapName: " + mapName, e);
        }
    }

    @Override
    public boolean removeListener(String listenerId) throws RedisDataStoreChangePropagatorException {
        cancelRunningTask(listenerId);
        return true;
    }

    private void validateAlreadyAttachedListener(String namespace) throws RedisDataStoreException {
        for (String id : runningTasks.keySet()) {
            if (id.equals(namespace))
                throw new RedisDataStoreException("One listener already attached on namespace: " + namespace);
        }
    }

    @Override
    public boolean isEmpty(String mapName) throws RedisDataStoreException {
        return size(mapName) == 0;
    }

    @Override
    public long size(String mapName) throws RedisDataStoreException {
        return redisReadOnlyClient.size(mapName);
    }

    @Override
    public boolean containsKey(String mapName, String key) throws RedisDataStoreException {
        return redisReadOnlyClient.containsKey(mapName, key);
    }

    @Override
    public RedisEntity<String, Object> get(String mapName, String key) throws RedisDataStoreException {
        int retry = 3;
        while (retry > 0) {
            try {
                return redisReadOnlyClient.get(mapName, key);
            } catch (Exception e) {
                log.error("get Failed with retry {} for {}", retry, mapName, e);
                retry--;
            }
        }
        throw new RedisDataStoreException("get Failed for " + mapName + " key " + key);
    }

    @Override
    public RedisEntity<String, Object> get(String mapName, String key, long eventTime) throws RedisDataStoreException {
        int retry = 3;
        while (retry > 0) {
            try {
                return redisReadOnlyClient.get(mapName, key, eventTime);
            } catch (Exception e) {
                log.error("get Failed with retry {} for {}", retry, mapName, e);
                retry--;
            }
        }
        throw new RedisDataStoreException("get Failed for " + mapName + " key " + key);
    }

    @Override
    public Map<String, RedisEntity<String, Object>> getBatch(String mapName, Set<String> keys) throws RedisDataStoreException {
        int retry = 3;
        while (retry > 0) {
            try {
                return redisReadOnlyClient.getBatch(mapName, keys);
            } catch (Exception e) {
                log.error("getBatch Failed with retry {} for {}", retry, mapName, e);
                retry--;
            }
        }
        throw new RedisDataStoreException("getBatch Failed for " + mapName + " keys " + keys.size());
    }

    @Override
    public Set<String> keySet(String mapName) throws RedisDataStoreException {
        int retry = 3;
        while (retry > 0) {
            try {
                return redisReadOnlyClient.keySet(mapName);
            } catch (Exception e) {
                log.error("keySet Failed with retry {} for {}", retry, mapName, e);
                retry--;
            }
        }
        throw new RedisDataStoreException("keySet Failed for " + mapName);
    }

    @Override
    public Map<String, RedisEntity<String, Object>> getAll(String mapName) throws RedisDataStoreException {
        int retry = 3;
        while (retry > 0) {
            try {
                return redisReadOnlyClient.getAll(mapName);
            } catch (Exception e) {
                log.error("getAll Failed with retry {} for {}", retry, mapName, e);
                retry--;
            }
        }
        throw new RedisDataStoreException("getAll Failed for " + mapName);
    }

    @Override
    public boolean mapNameExists(String mapName) throws RedisDataStoreException {
        try {
            return redisReadOnlyClient.mapNameExists(mapName);
        } catch (Exception e) {
            log.error("RedisDataStoreCDCImpl while checking for mapName existence: {}, {}", mapName, e);
            throw new RedisDataStoreException("RedisDataStoreCDCImpl while checking for mapName existence: " + mapName, e);
        }
    }

    @Override
    public boolean stop() throws RedisDataStoreException {
        try {
            log.info("Stopping redis datastore client");
            runningTasks.keySet().forEach(this::cancelRunningTask);
            executorService.shutdown();
            runningTasks.clear();
            return true;
        } catch (Exception e) {
            log.error("Error while closing the connection", e);
            return false;
        }
    }

    private void cancelRunningTask(String taskId) {
        ScheduledFuture<?> future = runningTasks.get(taskId);
        if (future != null && !future.isCancelled()) {
            future.cancel(false);
        }
        runningTasks.remove(taskId);
    }
}
