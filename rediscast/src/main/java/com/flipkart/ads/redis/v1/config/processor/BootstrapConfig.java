package com.flipkart.ads.redis.v1.config.processor;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class has the config required for bootstrap which is done while consumer initialisation
 */
public class BootstrapConfig {
    // Core number of threads in the executor service
    private int corePoolSize = 10;
    // Maximum number of threads in the executor service
    private int maxPoolSize = 50;
    // Maximum number of events in one batch while bootstrapping
    private int batchSize = 50000;

    public BootstrapConfig(@JsonProperty("corePoolSize") int corePoolSize, @JsonProperty("maxPoolSize") int maxPoolSize, @JsonProperty("batchSize") int batchSize) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.batchSize = batchSize;
        this.validate();
    }

    public BootstrapConfig() {
    }

    private void validate() {
        if (this.corePoolSize > 10) {
            throw new IllegalArgumentException("corePoolSize shouldn't be greater than 10");
        } else if (this.batchSize > 100000) {
            throw new IllegalArgumentException("batch size above 100000 not allowed");
        }
    }

    public int getCorePoolSize() {
        return this.corePoolSize;
    }

    public int getMaxPoolSize() {
        return this.maxPoolSize;
    }

    public int getBatchSize() {
        return this.batchSize;
    }
}
