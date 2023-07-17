package com.flipkart.ads.redis.v1.event;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.ads.redis.v1.config.processor.BootstrapConfig;
import com.flipkart.ads.redis.v1.stream.RedisDataStoreCDC;

import java.io.Closeable;

import static java.util.Objects.isNull;

public interface RedisDataStoreEventProcessor extends Closeable {
    void init() throws Exception;

    boolean isInitialised();

    class RedisDataStoreEventProcessorBuilder {
        private final String mapName;
        private final RedisDataStoreCDC redisDataStoreCDC;
        private final MetricRegistry metricRegistry;
        private RedisDataStoreChangePropagator cdcPropagator;
        private Integer stripes;
        private BootstrapConfig bootstrapConfig;

        public RedisDataStoreEventProcessorBuilder(String mapName, RedisDataStoreCDC redisDataStoreCDC,
                                                   MetricRegistry metricRegistry) {
            this.mapName = mapName;
            this.redisDataStoreCDC = redisDataStoreCDC;
            this.metricRegistry = metricRegistry;
        }

        public RedisDataStoreEventProcessor.RedisDataStoreEventProcessorBuilder addChangePropagator(RedisDataStoreChangePropagator<String, Object> changePropagator) {
            this.cdcPropagator = changePropagator;
            return this;
        }

        public RedisDataStoreEventProcessor.RedisDataStoreEventProcessorBuilder withStripeLock(int stripes) {
            if (stripes <= 0) {
                throw new IllegalArgumentException("Number of stripes should be greater than 0!!");
            }
            this.stripes = stripes;
            return this;
        }

        public RedisDataStoreEventProcessor.RedisDataStoreEventProcessorBuilder withIncrementalBootstrap(BootstrapConfig bootstrapConfig) {
            if (bootstrapConfig == null) {
                throw new IllegalArgumentException("Bootstrap config is null");
            }
            this.bootstrapConfig = bootstrapConfig;
            return this;
        }

        public RedisDataStoreEventProcessor.RedisDataStoreEventProcessorBuilder withIncrementalBootstrap() {
            this.bootstrapConfig = new BootstrapConfig();
            return this;
        }

        public RedisDataStoreEventProcessor build() {
            if (isNull(this.stripes)) {
                return new RedisDataStoreEventProcessorImpl(redisDataStoreCDC, cdcPropagator,
                        mapName, metricRegistry, bootstrapConfig);
            } else {
                return new RedisDataStoreEventProcessorImpl(redisDataStoreCDC, cdcPropagator,
                        mapName, stripes, metricRegistry, bootstrapConfig);
            }
        }
    }
}
