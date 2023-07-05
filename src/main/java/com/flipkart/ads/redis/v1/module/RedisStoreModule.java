package com.flipkart.ads.redis.v1.module;

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.codahale.metrics.InstrumentedThreadFactory;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.ads.redis.v1.cache.RedisDataInMemoryCache;
import com.flipkart.ads.redis.v1.client.*;
import com.flipkart.ads.redis.v1.datastore.DataStore;
import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.handler.RedisStoreHandler;
import com.flipkart.ads.redis.v1.ingestion.stategy.RedisInitialisationStrategy;
import com.flipkart.ads.redis.v1.ingestion.stategy.RedisStoreCacheInitialisationStrategy;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.flipkart.ads.redis.v1.pool.JedisSlavePool;
import com.flipkart.ads.redis.v1.pool.RedisPoolConfig;
import com.flipkart.ads.redis.v1.providers.RedisCacheProvider;
import com.flipkart.ads.redis.v1.providers.RedisDataStoreProvider;
import com.flipkart.ads.redis.v1.stream.RedisDataStoreCDC;
import com.flipkart.ads.redis.v1.stream.RedisDataStoreCDCImpl;
import com.flipkart.ads.redis.v1.transformers.RedisTransformer;
import com.flipkart.ads.redis.v1.utils.PoolUtils;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

@RequiredArgsConstructor
public class RedisStoreModule extends AbstractModule {
    private final boolean isOptionalRedisBindingNeeded;

    public RedisStoreModule() {
        this.isOptionalRedisBindingNeeded = true;
    }

    @Override
    protected void configure() {
        bindDataStoreHandlers();
        bindCacheProviders();
        bindInitialisationStrategy();

        if (isOptionalRedisBindingNeeded) {
            bindOptionalListenerMap();
        }
    }

    @Provides
    @Singleton
    public RedisDataStoreCDC getIngestionClient(MetricRegistry metricRegistry,
                                                @Named("redis_read_only_client") RedisReadOnlyClient redisReadOnlyClient,
                                                @Named("redisCacheScheduler") ScheduledExecutorService executorService) {
        return new RedisDataStoreCDCImpl(metricRegistry, redisReadOnlyClient, executorService);
    }

    @Provides
    @Named("redisCacheScheduler")
    @Singleton
    public ScheduledExecutorService providerScheduledExecutorService(final MetricRegistry registry) {
        ThreadFactory threadFactory = new InstrumentedThreadFactory(
                new ThreadFactoryBuilder().setNameFormat("RedisCacheScheduler").build(),
                registry, "RedisCacheScheduler.ThreadFactory");

        return new InstrumentedScheduledExecutorService(Executors.newScheduledThreadPool(20, threadFactory), registry, "RedisCacheScheduler");
    }

    @Provides
    @Singleton
    @Named("redis_read_only_client")
    public RedisReadOnlyClient getRedisDataStoreDao(@Named("redis_slave_pool") JedisSlavePool jedisSlavePool,
                                                    Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> mapNameToEntityStreamConfig,
                                                    Map<RedisMap, RedisTransformer> mapNameToEntityDataTransformers,
                                                    MetricRegistry metricRegistry) {
        return new GenericRedisReadOnlyClient(jedisSlavePool, mapNameToEntityStreamConfig, mapNameToEntityDataTransformers,
                metricRegistry);
    }

    @Provides
    @Singleton
    @Named("redis_write_only_client")
    public RedisWriteOnlyClient provideRedisWriteOnlyClient(@Named("redis_master_pool") Pool<Jedis> jedis,
                                                            Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> mapNameToEntityStreamConfig,
                                                            Map<RedisMap, RedisTransformer> mapNameToEntityDataTransformers,
                                                            MetricRegistry metricRegistry) {
        return new GenericRedisWriteOnlyClient(jedis, mapNameToEntityStreamConfig, mapNameToEntityDataTransformers, metricRegistry);
    }

    @Provides
    @Singleton
    @Named("redis_slave_pool")
    public JedisSlavePool getBannerMapJedisSlavePool(@Named("redis_stream_config") RedisPoolConfig redisStreamConfig) {
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(redisStreamConfig.getMaxThreads());
        genericObjectPoolConfig.setMaxWaitMillis(redisStreamConfig.getMaxWaitInMillis());
        return new JedisSlavePool(redisStreamConfig.getMasterNameNPassword(), genericObjectPoolConfig,
                redisStreamConfig.getSentinel(), redisStreamConfig.getTimeout(), redisStreamConfig.getSlaveListenerThreadWaitTimeMillis());
    }

    @Provides
    @Singleton
    @Named("redis_master_pool")
    public Pool<Jedis> provideStreamJedisPool(@Named("redis_stream_config") RedisPoolConfig redisPoolConfig) {
        return PoolUtils.getJedisSentinelPool(redisPoolConfig);
    }

    @Provides
    @Singleton
    public Set<RedisDataInMemoryCache> getRedisCacheMaps(@Named("redis_cache_map") Map<RedisMap, Object> redisMaps) {
        Set<RedisDataInMemoryCache> redisDataStoreCachesSet = Sets.newHashSet();
        if (MapUtils.isNotEmpty(redisMaps)) {
            redisMaps.forEach((redisMap, value) -> {
                if (value instanceof RedisDataInMemoryCache)
                    redisDataStoreCachesSet.add((RedisDataInMemoryCache) value);
            });
        }
        return redisDataStoreCachesSet;
    }

    @Provides
    @Singleton
    public Map<RedisMap, RedisDataInMemoryCache> getRedisDataStoreCaches(@Named("redis_cache_map") Map<RedisMap, Object> redisCaches) {
        Map<RedisMap, RedisDataInMemoryCache> redisDataStoreCacheMap = new HashMap<>();
        if (MapUtils.isNotEmpty(redisCaches)) {
            redisCaches.forEach((redisMap, cache) -> {
                if (cache instanceof RedisDataInMemoryCache) {
                    redisDataStoreCacheMap.putIfAbsent(redisMap, (RedisDataInMemoryCache) cache);
                }
            });
        }
        return redisDataStoreCacheMap;
    }

    @Provides
    @Singleton
    public DataStore<?> getDataStore(RedisDataStoreProvider redisDataStoreProvider) {
        return redisDataStoreProvider.provideDataStore();
    }

    @Provides
    @Singleton
    @Named("redis_cache_map")
    public Map<RedisMap, Object> getRedisCaches(RedisCacheProvider redisCacheProvider) {
        return redisCacheProvider.provideCaches();
    }

    private void bindDataStoreHandlers() {
        bind(RedisDataStoreProvider.class).to(RedisStoreHandler.class).in(Singleton.class);
    }

    private void bindCacheProviders() {
        bind(RedisCacheProvider.class).to(RedisStoreHandler.class).in(Singleton.class);
    }

    private void bindInitialisationStrategy() {
        bind(RedisInitialisationStrategy.class).to(RedisStoreCacheInitialisationStrategy.class).in(Singleton.class);
    }

    private void bindOptionalListenerMap() {
        TypeLiteral<RedisMap> redisMapTypeLiteral = new TypeLiteral<RedisMap>() {
        };
        TypeLiteral<RedisDataStoreChangePropagator> changePropagatorTypeLiteral
                = new TypeLiteral<RedisDataStoreChangePropagator>() {
        };
        MapBinder.newMapBinder(binder(), redisMapTypeLiteral, changePropagatorTypeLiteral);
    }
}
