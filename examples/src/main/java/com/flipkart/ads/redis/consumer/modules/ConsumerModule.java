package com.flipkart.ads.redis.consumer.modules;

import com.flipkart.ads.redis.consumer.listener.SampleEntityListener;
import com.flipkart.ads.redis.models.SampleRedisMap;
import com.flipkart.ads.redis.transformers.SampleEntityTransformer;
import com.flipkart.ads.redis.v1.client.AbstractRedisClient;
import com.flipkart.ads.redis.v1.config.locks.StripedMultiLockConfig;
import com.flipkart.ads.redis.v1.config.processor.BootstrapConfig;
import com.flipkart.ads.redis.v1.config.processor.RedisEventProcessorConfig;
import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.flipkart.ads.redis.v1.pool.RedisPoolConfig;
import com.flipkart.ads.redis.v1.transformers.RedisTransformer;
import com.google.common.collect.Sets;
import com.google.inject.Module;
import com.google.inject.*;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerModule implements Module {
    @Override
    public void configure(Binder binder) {
        bindIngestionListeners(binder);
        bindMapToTransformers(binder);
    }

    // bind the listeners for maps for which client wants to get notified of any events
    // If client doesn't want to process the event externally and only local copy is required, then no binding is needed here
    private void bindIngestionListeners(Binder binder) {
        MapBinder<RedisMap, RedisDataStoreChangePropagator> listenerMapBinder
                = MapBinder.newMapBinder(binder, new TypeLiteral<RedisMap>() {
                },
                new TypeLiteral<RedisDataStoreChangePropagator>() {
                });

        listenerMapBinder.addBinding(SampleRedisMap.SAMPLE_MAP_ONE).to(SampleEntityListener.class).in(Singleton.class);
        listenerMapBinder.addBinding(SampleRedisMap.SAMPLE_MAP_TWO).to(SampleEntityListener.class).in(Singleton.class);
    }

    private void bindMapToTransformers(Binder binder) {
        MapBinder<RedisMap, RedisTransformer> transformerMapBinder
                = MapBinder.newMapBinder(binder, new TypeLiteral<RedisMap>() {
        }, new TypeLiteral<RedisTransformer>() {
        });

        transformerMapBinder.addBinding(SampleRedisMap.SAMPLE_MAP_ONE).to(SampleEntityTransformer.class).in(Singleton.class);
        transformerMapBinder.addBinding(SampleRedisMap.SAMPLE_MAP_TWO).to(SampleEntityTransformer.class).in(Singleton.class);
    }

    @Provides
    @Singleton
    @Named("redis_stream_config")
    public RedisPoolConfig getRedisSlavePoolConfig() {
        RedisPoolConfig redisPoolConfig = new RedisPoolConfig();

        redisPoolConfig.setSentinel(Sets.newHashSet("127.0.0.1:26379"));
        redisPoolConfig.setMasterNameNPassword("sample-master");
        redisPoolConfig.setMaxThreads(8);

        return redisPoolConfig;
    }

    // Provide the list of maps for which client wants to listen
    @Provides
    @Singleton
    private List<RedisMap> provideRedisMaps() {
        return Arrays.asList(SampleRedisMap.SAMPLE_MAP_ONE, SampleRedisMap.SAMPLE_MAP_TWO);
    }

    // Provide the stream config
    @Provides
    @Singleton
    private Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> provideMapToEntityStreamConfigs() {
        Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> mapNameToEntityType = new HashMap<>();
        mapNameToEntityType.put(SampleRedisMap.SAMPLE_MAP_ONE, new AbstractRedisClient.EntityStreamConfigs(SampleRedisMap.SAMPLE_MAP_ONE.getMapName(), 1000, 5000, 100000));
        mapNameToEntityType.put(SampleRedisMap.SAMPLE_MAP_TWO, new AbstractRedisClient.EntityStreamConfigs(SampleRedisMap.SAMPLE_MAP_TWO.getMapName(), 1000, 5000, 1000));

        return mapNameToEntityType;
    }

    // Provide bootstrap config for each map. Whatever maps present here, only these maps will be listened to for the events from redis
    @Provides
    @Singleton
    private Map<RedisMap, RedisEventProcessorConfig> provideBootstrapConfig(List<RedisMap> redisMaps) {
        Map<RedisMap, RedisEventProcessorConfig> redisEventProcessorConfigMap = new HashMap<>();

        for (RedisMap redisMap : redisMaps) {
            redisEventProcessorConfigMap.put(redisMap, getBootstrapConfig((SampleRedisMap) redisMap));
        }

        return redisEventProcessorConfigMap;
    }

    private RedisEventProcessorConfig getBootstrapConfig(SampleRedisMap redisMap) {
        RedisEventProcessorConfig.RedisEventProcessorConfigBuilder redisEventProcessorConfigBuilder =
                RedisEventProcessorConfig.builder();
        StripedMultiLockConfig stripedMultiLockConfig = new StripedMultiLockConfig(15000);
        redisEventProcessorConfigBuilder.lockConfig(stripedMultiLockConfig);

        switch (redisMap) {
            case SAMPLE_MAP_ONE:
            case SAMPLE_MAP_TWO:
                redisEventProcessorConfigBuilder.bootstrapConfig(new BootstrapConfig(10, 50, 500));
                redisEventProcessorConfigBuilder.localCopyRequired(true);
                return redisEventProcessorConfigBuilder.build();
        }
        return redisEventProcessorConfigBuilder.build();
    }
}
