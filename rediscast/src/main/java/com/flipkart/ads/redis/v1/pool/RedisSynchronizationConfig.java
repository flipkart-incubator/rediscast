package com.flipkart.ads.redis.v1.pool;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RedisSynchronizationConfig {
    @JsonProperty("wait_timeout")
    private long waitTimeout = 0;

    @JsonProperty("wait_replicas")
    private int replicas = 1;

    @JsonProperty("wait_retries")
    private int retries = 3;
}
