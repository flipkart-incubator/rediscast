package com.flipkart.ads.redis.v1.handler;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.ads.redis.v1.cache.RedisDataInMemoryCache;
import com.flipkart.ads.redis.v1.cache.RedisDataInMemoryCacheBuilder;
import com.flipkart.ads.redis.v1.config.locks.StripedMultiLockConfig;
import com.flipkart.ads.redis.v1.config.processor.RedisEventProcessorConfig;
import com.flipkart.ads.redis.v1.datastore.DataStore;
import com.flipkart.ads.redis.v1.datastore.RedisDataStoreImpl;
import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.event.RedisDataStoreEventProcessor;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.flipkart.ads.redis.v1.providers.RedisCacheProvider;
import com.flipkart.ads.redis.v1.providers.RedisDataStoreProvider;
import com.flipkart.ads.redis.v1.stream.RedisDataStoreCDC;
import com.google.inject.Inject;
import com.google.inject.Provider;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class RedisStoreHandler implements RedisDataStoreProvider, RedisCacheProvider {
    private final Provider<RedisDataStoreCDC> redisDataStoreProvider;
    private final Provider<Map<RedisMap, RedisDataStoreChangePropagator>> listenerProvider;
    private final Provider<Map<RedisMap, RedisEventProcessorConfig>> mapsToInitialise;
    private final DataStore<RedisDataInMemoryCache> redisDataStore;
    private final MetricRegistry metricRegistry;

    @Inject
    public RedisStoreHandler(Provider<RedisDataStoreCDC> redisDataStoreProvider,
                             Provider<Map<RedisMap, RedisEventProcessorConfig>> mapsToInitialise,
                             Provider<Map<RedisMap, RedisDataStoreChangePropagator>> listenerProvider,
                             MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        this.redisDataStore = new RedisDataStoreImpl();
        this.mapsToInitialise = mapsToInitialise;
        this.redisDataStoreProvider = redisDataStoreProvider;
        this.listenerProvider = listenerProvider;
    }

    @Override
    public Map<RedisMap, Object> provideCaches() {
        RedisDataStoreCDC redisDataStoreCDC = redisDataStoreProvider.get();

        Map<RedisMap, Object> caches = new LinkedHashMap<>();

        for (RedisMap map : mapsToInitialise.get().keySet()) {
            log.debug("Creating cache object for map name: {}", map.getMapName());
            RedisEventProcessorConfig config = mapsToInitialise.get().get(map);

            RedisDataInMemoryCacheBuilder redisDataInMemoryCacheBuilder = new RedisDataInMemoryCacheBuilder(map.getMapName(), redisDataStoreCDC, metricRegistry);

            redisDataInMemoryCacheBuilder.withIncrementalBootstrap(config.getBootstrapConfig());
            redisDataInMemoryCacheBuilder.withStripeLock(((StripedMultiLockConfig) config.getLockConfig()).getStripes());

            if (!config.isLocalCopyRequired()) {
                redisDataInMemoryCacheBuilder.withoutLocalCopy();
            }

            Map<RedisMap, RedisDataStoreChangePropagator> listeners = listenerProvider.get();
            if (MapUtils.isNotEmpty(listeners)) {
                if (listeners.get(map) != null) {
                    redisDataInMemoryCacheBuilder.addChangePropagator(listeners.get(map));
                }
            }

            Object cache = redisDataInMemoryCacheBuilder.build();
            caches.put(map, cache);
        }

        return caches;
    }

    @Override
    public DataStore<?> provideDataStore() {
        return redisDataStore;
    }

    private Gauge<Long> getCacheSizeGauge(Object redisDataStoreCache, String mapName) {
        return () -> provideSize(redisDataStoreCache, mapName);
    }

    private Long provideSize(Object redisDataStoreCache, String mapName) {
        if (redisDataStoreCache instanceof RedisDataInMemoryCache)
            return ((RedisDataInMemoryCache) redisDataStoreCache).size();
        else if (redisDataStoreCache instanceof RedisDataStoreEventProcessor) {
            log.debug("Creating dummy size for processor types: {}", mapName);
            return 0L;
        } else {
            log.debug("Not known type for: {}", mapName);
            return 0L;
        }
    }
}
