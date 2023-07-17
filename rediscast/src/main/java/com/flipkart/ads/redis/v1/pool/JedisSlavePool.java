package com.flipkart.ads.redis.v1.pool;

import com.flipkart.ads.redis.v1.utils.RedisUtils;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class JedisSlavePool extends JedisPoolAbstract {
    private static final int DEFAULT_DATABASE = 0;
    private static final int MINIMUM_SLAVE_PRIORITY = 0;
    private final AtomicBoolean initInProgress = new AtomicBoolean(false);
    protected int database = Protocol.DEFAULT_DATABASE;
    private String masterName;
    private String masterPassword;
    private GenericObjectPoolConfig poolConfig;
    private Set<String> sentinels;
    private int timeoutMs;
    private Set<JedisShardInfo> shards = new HashSet<>();
    private long slaveListenerThreadWaitTimeMillis;
    private Set<SlaveListener> slaveListeners = new HashSet<>();

    @Inject
    public JedisSlavePool(String masterNameNPassword, final GenericObjectPoolConfig poolConfig, Set<String> sentinels, int timeout, long slaveListenerThreadWaitTimeMillis) {
        Map.Entry<String, String> masterNPassword = RedisUtils.toMasterAndPassword(masterNameNPassword);
        this.masterName = masterNPassword.getKey();
        this.masterPassword = masterNPassword.getValue();
        this.poolConfig = poolConfig;
        this.sentinels = sentinels;
        this.timeoutMs = timeout;
        this.slaveListenerThreadWaitTimeMillis = slaveListenerThreadWaitTimeMillis;
        addSlaveListenerToSentinel();
        initPool();
    }

    public void initPool() {
        if (initInProgress.get()) {
            return;
        }
        synchronized (initInProgress) {
            initInProgress.set(true);
            this.shards = getJedisShards();
            initPool(poolConfig, new RoundRobinFactory(shards, database));
            initInProgress.set(false);
        }
    }

    private Set<JedisShardInfo> getJedisShards() {
        Set<JedisShardInfo> shards = new HashSet<>();
        for (String sentinel : sentinels) {
            final HostAndPort hap = RedisUtils.toHostAndPort(sentinel);
            try (Jedis jedis = new Jedis(hap.getHost(), hap.getPort())) {
                final List<Map<String, String>> slaves = jedis.sentinelSlaves(masterName);
                shards.addAll(getShards(slaves, timeoutMs));
            } catch (Exception e) {
                log.error("Sentinel slave fetch issue {}", hap, e);
            }
        }
        return shards;
    }

    private void addSlaveListenerToSentinel() {
        for (String sentinel : sentinels) {
            final HostAndPort hap = RedisUtils.toHostAndPort(sentinel);
            SlaveListener slaveListener = new SlaveListener(hap.getHost(), hap.getPort(), masterName, d -> false, d -> initPool(), slaveListenerThreadWaitTimeMillis);
            // whether SlaveListener threads are alive or not, process can be stoppedq  1aq
            slaveListener.setDaemon(true);
            slaveListeners.add(slaveListener);
            slaveListener.start();
        }
    }

    @Override
    public Jedis getResource() {
        Jedis jedis = super.getResource();
        jedis.setDataSource(this);
        return jedis;
    }

    public List<JedisShardInfo> getShards(List<Map<String, String>> slaves, int timeout) {
        List<JedisShardInfo> shards = new ArrayList<>();
        for (Map<String, String> slaveInfo : slaves) {
            String host = slaveInfo.get("ip");
            int port = Integer.parseInt(slaveInfo.get("port"));
            Jedis jedis = new Jedis(host, port);
            if (masterPassword != null)
                jedis.auth(masterPassword);
            int slavePriority = Integer.parseInt(slaveInfo.get("slave-priority"));
            if (RedisUtils.isHostReachable(jedis) && slavePriority > MINIMUM_SLAVE_PRIORITY) {
                JedisShardInfo jedisShard = new JedisShardInfo(host, port, timeout);
                if (masterPassword != null)
                    jedisShard.setPassword(masterPassword);
                shards.add(jedisShard);
            } else {
                log.warn("Not including slave, {} with {} slave priority. Minimum slave priority {}.", host, slavePriority, MINIMUM_SLAVE_PRIORITY);
            }
        }
        return shards;
    }

    public void close() {
        for (SlaveListener slaveL : slaveListeners) {
            slaveL.shutdown();
        }
        super.close();
    }

    private static class RoundRobinFactory implements PooledObjectFactory<Jedis> {
        private final List<JedisShardInfo> shards = new ArrayList<>();
        private final Iterator<JedisShardInfo> shardIterator;
        private final int database;

        public RoundRobinFactory(Set<JedisShardInfo> shards, int database) {
            this.shards.addAll(shards);
            this.shardIterator = Iterators.cycle(this.shards);
            this.database = database;
        }

        public PooledObject<Jedis> makeObject() {
            JedisShardInfo jsi;
            synchronized (shardIterator) {
                jsi = Iterators.getNext(shardIterator, shards.get(0));
            }
            assert jsi != null;
            Jedis jedis = new Jedis(jsi.getHost(), jsi.getPort());
            if (jsi.getPassword() != null) {
                jedis.auth(jsi.getPassword());
            }
            if (database != DEFAULT_DATABASE) jedis.select(database);
            return new DefaultPooledObject<>(jedis);
        }

        public void destroyObject(PooledObject<Jedis> jedis) {
            try {
                try {
                    jedis.getObject().quit();
                } catch (Exception e) {
                    log.error("Exception occur while closing connection with redis : {}", e.getMessage(), e);
                }
                jedis.getObject().disconnect();
            } catch (Exception e) {
                log.error("Exception occur while disconnecting from redis : {}", e.getMessage(), e);
            }
        }

        public boolean validateObject(PooledObject<Jedis> jedis) {
            try {
                return jedis.getObject().ping().equals("PONG");
            } catch (Exception ex) {
                log.error("Could not reach the given slave : {}", ex.getMessage());
                return false;
            }
        }

        @Override
        public void activateObject(PooledObject<Jedis> p) {
        }

        @Override
        public void passivateObject(PooledObject<Jedis> p) {
        }
    }
}
