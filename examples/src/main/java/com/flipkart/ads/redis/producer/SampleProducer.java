package com.flipkart.ads.redis.producer;

import com.flipkart.ads.redis.models.SampleEntity;
import com.flipkart.ads.redis.v1.client.RedisWriteOnlyClient;
import com.flipkart.ads.redis.v1.exceptions.RedisWriteSynchronizationException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.pool.RedisSynchronizationConfig;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class SampleProducer {
    private final RedisWriteOnlyClient redisWriteOnlyClient;

    // redis write only client will automatically be injected as these are created inside a module in the library by default
    @Inject
    public SampleProducer(@Named("redis_write_only_client") RedisWriteOnlyClient redisWriteOnlyClient) {
        this.redisWriteOnlyClient = redisWriteOnlyClient;
    }

    public void produce() {
        SampleEntity sampleEntity = new SampleEntity("typeOne", "valueOne");

        // Define synchronization config to write to redis synchronously upto n number of slaves. this can be used to avpid data loss in case of master goes down
        RedisSynchronizationConfig redisSynchronizationConfig = new RedisSynchronizationConfig();
        redisSynchronizationConfig.setReplicas(1);
        redisSynchronizationConfig.setRetries(1);
        redisSynchronizationConfig.setWaitTimeout(200);

        // Create entity with required params. type is map name here which is defined in SampleRedisMap
        RedisEntity<String, Object> redisEntity = new RedisEntity<>("sampleMapOne", "keyOne", System.currentTimeMillis());
        redisEntity.setValue(sampleEntity);

        try {
            // Call back can be passed if some logic has to be done after writing
            redisWriteOnlyClient.updateInRedisSynchronously(redisEntity, null, redisSynchronizationConfig);
        } catch (RedisWriteSynchronizationException e) {
            throw new RuntimeException(e);
        }
    }
}
