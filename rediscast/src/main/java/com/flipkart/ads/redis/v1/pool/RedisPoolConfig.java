package com.flipkart.ads.redis.v1.pool;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RedisPoolConfig {
    private List<String> hosts;
    private String masterNameNPassword;
    private Set<String> sentinel;
    private int timeout = 500;
    private long expirySec = TimeUnit.MINUTES.toSeconds(30);
    private long expiryOnEmptySec = TimeUnit.HOURS.toSeconds(2);
    private int maxThreads;
    private long maxWaitInMillis;
    private long slaveListenerThreadWaitTimeMillis = 10000;
}
