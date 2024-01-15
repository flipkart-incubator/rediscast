package com.flipkart.ads.redis.producer.modules;

import com.flipkart.ads.redis.models.SampleRedisMap;
import com.flipkart.ads.redis.transformers.SampleEntityTransformer;
import com.flipkart.ads.redis.v1.client.AbstractRedisClient;
import com.flipkart.ads.redis.v1.config.processor.RedisEventProcessorConfig;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.flipkart.ads.redis.v1.pool.RedisPoolConfig;
import com.flipkart.ads.redis.v1.transformers.RedisTransformer;
import com.google.common.collect.Sets;
import com.google.inject.Module;
import com.google.inject.*;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;

import java.util.HashMap;
import java.util.Map;

public class ProducerModule implements Module {
    @Override
    public void configure(Binder binder) {
        bindMapToTransformers(binder);
    }

    // Bind transformers needed for each map
    private void bindMapToTransformers(Binder binder) {
        MapBinder<RedisMap, RedisTransformer> transformerMapBinder
                = MapBinder.newMapBinder(binder, new TypeLiteral<RedisMap>() {
        }, new TypeLiteral<RedisTransformer>() {
        });

        transformerMapBinder.addBinding(SampleRedisMap.SAMPLE_MAP_ONE).to(SampleEntityTransformer.class).in(Singleton.class);
        transformerMapBinder.addBinding(SampleRedisMap.SAMPLE_MAP_TWO).to(SampleEntityTransformer.class).in(Singleton.class);
        transformerMapBinder.addBinding(SampleRedisMap.SAMPLE_MAP_THREE).to(SampleEntityTransformer.class).in(Singleton.class);
    }

    // Bind stream configs for each map
    @Provides
    @Singleton
    public Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> provideMapToEntityStreamConfigs() {
        Map<RedisMap, AbstractRedisClient.EntityStreamConfigs> mapNameToEntityType = new HashMap<>();
        mapNameToEntityType.put(SampleRedisMap.SAMPLE_MAP_ONE, new AbstractRedisClient.EntityStreamConfigs(SampleRedisMap.SAMPLE_MAP_ONE.getMapName(), 1000, 5000, 300000));
        mapNameToEntityType.put(SampleRedisMap.SAMPLE_MAP_TWO, new AbstractRedisClient.EntityStreamConfigs(SampleRedisMap.SAMPLE_MAP_TWO.getMapName(), 1000, 5000, 200000));
        mapNameToEntityType.put(SampleRedisMap.SAMPLE_MAP_THREE, new AbstractRedisClient.EntityStreamConfigs(SampleRedisMap.SAMPLE_MAP_THREE.getMapName(), 1000, 5000, 100000));

        return mapNameToEntityType;
    }

    // Redis config which will be used to initialise redis clients in the library
    @Provides
    @Singleton
    @Named("redis_stream_config")
    public RedisPoolConfig provideStreamRedisConfig() {
        RedisPoolConfig redisPoolConfig = new RedisPoolConfig();

        redisPoolConfig.setSentinel(Sets.newHashSet("127.0.0.1:26379"));
        redisPoolConfig.setMasterNameNPassword("sample-master");
        redisPoolConfig.setMaxThreads(8);

        return redisPoolConfig;
    }

    // Provide this if some maps are getting consumed in the same service. empty map can be provided for just the producer
    @Provides
    @Singleton
    public Map<RedisMap, RedisEventProcessorConfig> provideRedisEventProcessorConfig() {
        return new HashMap<>();
    }
}
