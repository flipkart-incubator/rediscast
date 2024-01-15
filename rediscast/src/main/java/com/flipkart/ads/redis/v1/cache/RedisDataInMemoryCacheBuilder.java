package com.flipkart.ads.redis.v1.cache;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.config.processor.BootstrapConfig;
import com.flipkart.ads.redis.v1.stream.RedisDataStoreCDC;

import static java.util.Objects.isNull;

public class RedisDataInMemoryCacheBuilder {
    private final String mapName;
    private final MetricRegistry metricRegistry;
    private final RedisDataStoreCDC redisDataStoreCDC;
    private RedisDataStoreChangePropagator cdcPropagator;
    private boolean localCopyRequired = true;
    private boolean withTtlSupport = false;
    private Integer stripes;
    private BootstrapConfig bootstrapConfig;

    public RedisDataInMemoryCacheBuilder(String mapName, RedisDataStoreCDC redisDataStoreCDC, MetricRegistry metricRegistry) {
        this.mapName = mapName;
        this.redisDataStoreCDC = redisDataStoreCDC;
        this.metricRegistry = metricRegistry;
    }

    public RedisDataInMemoryCacheBuilder addChangePropagator(RedisDataStoreChangePropagator<String, Object> cdcPropagator) {
        this.cdcPropagator = cdcPropagator;
        return this;
    }

    public RedisDataInMemoryCacheBuilder withoutLocalCopy() {
        this.localCopyRequired = false;
        return this;
    }

    public RedisDataInMemoryCacheBuilder withTtlSupport() {
        this.withTtlSupport = true;
        return this;
    }

    public RedisDataInMemoryCacheBuilder withStripeLock(int stripes) {
        if (stripes <= 0) {
            throw new IllegalArgumentException("Number of stripes should be greater than 0!!");
        }
        this.stripes = stripes;
        return this;
    }

    public RedisDataInMemoryCacheBuilder withIncrementalBootstrap(BootstrapConfig bootstrapConfig) {
        if (bootstrapConfig == null) {
            throw new IllegalArgumentException("Bootstrap config is null");
        }
        this.bootstrapConfig = bootstrapConfig;
        return this;
    }

    public RedisDataInMemoryCacheBuilder withIncrementalBootstrap() {
        this.bootstrapConfig = new BootstrapConfig();
        return this;
    }

    public RedisDataInMemoryCache build() {
        RedisInMemoryMap localMap = null;
        if (this.localCopyRequired) {
            localMap = this.withTtlSupport ? new RedisInMemoryMap(RedisInMemoryMap.RedisLocalMapType.LOCAL_DATA_WITH_TTL_SUPPORTED, this.mapName) : new RedisInMemoryMap(RedisInMemoryMap.RedisLocalMapType.LOCAL_DATA_MAP, this.mapName);
        } else {
            localMap = new RedisInMemoryMap(RedisInMemoryMap.RedisLocalMapType.MINIMAL_DATA_MAP, this.mapName);
        }
        if (isNull(this.stripes)) {
            return new RedisDataInMemoryCacheImpl(mapName, localMap, redisDataStoreCDC, cdcPropagator, bootstrapConfig, metricRegistry);
        } else {
            return new RedisDataInMemoryCacheImpl(mapName, localMap, redisDataStoreCDC, cdcPropagator, stripes, bootstrapConfig, metricRegistry);
        }
    }
}
