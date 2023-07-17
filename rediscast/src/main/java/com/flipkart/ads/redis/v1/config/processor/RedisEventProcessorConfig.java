package com.flipkart.ads.redis.v1.config.processor;

import com.flipkart.ads.redis.v1.config.locks.LockConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@EqualsAndHashCode
@Builder
@Getter
public class RedisEventProcessorConfig {
    BootstrapConfig bootstrapConfig;
    LockConfig lockConfig;
    boolean localCopyRequired;
    boolean isParallelProcessingEnabledForUpdates;
}
