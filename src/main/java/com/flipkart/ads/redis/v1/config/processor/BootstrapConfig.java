package com.flipkart.ads.redis.v1.config.processor;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BootstrapConfig {
    private int corePoolSize = 10;
    private int maxPoolSize = 50;
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
