package com.flipkart.ads.redis.v1.utils;

import com.flipkart.ads.redis.v1.pool.RedisPoolConfig;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.util.Pool;

import java.util.Map;

@Slf4j
public class PoolUtils {
    public static Pool<Jedis> getJedisSentinelPool(RedisPoolConfig redisPoolConfig) {
        try {
            Map.Entry<String, String> masterNPassword = RedisUtils.toMasterAndPassword(redisPoolConfig.getMasterNameNPassword());
            String masterToUse = masterNPassword.getKey();
            String masterPassword = masterNPassword.getValue();
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMinIdle(1);
            return new JedisSentinelPool(masterToUse, Sets.newHashSet(redisPoolConfig.getSentinel()), poolConfig, redisPoolConfig.getTimeout(), masterPassword);
        } catch (Exception exp) {
            log.error("Error in creating jedis sentinel pool: {}", exp.getMessage());
            throw exp;
        }
    }
}
