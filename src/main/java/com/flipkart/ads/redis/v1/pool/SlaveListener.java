package com.flipkart.ads.redis.v1.pool;

import com.flipkart.ads.redis.v1.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class SlaveListener extends Thread {
    private final static String POSITIVE_SDOWN = "+sdown";
    private final static String NEGATIVE_SDOWN = "-sdown";
    private final static String NEGATIVE_ODOWN = "-odown";
    private final static String POSTIVE_ODOWN = "+odown";
    private final static String POSTIVE_SLAVE = "+slave";
    private final static String POSITIVE_RESET_MASTER = "+reset-master";
    private final static String POSITIVE_SWITCH_MASTER = "+switch-master";
    protected final String masterName;
    protected final String host;
    protected final int port;
    protected final long threadWaitTimeMillis;
    private final Function<Void, Boolean> isSentinelResetEnabled;
    private final Consumer<Void> callBack;
    private final AtomicLong lastResetDelay = new AtomicLong(0);
    protected AtomicBoolean running = new AtomicBoolean(false);
    private Jedis jedis;

    public SlaveListener(String host, int port, String masterName, Function<Void, Boolean> isSentinelResetEnabled, Consumer<Void> callBack, long threadWaitTimeMillis) {
        super(String.format("SlaveListener-%s-[%s:%d]", masterName, host, port));
        this.masterName = masterName;
        this.host = host;
        this.port = port;
        this.threadWaitTimeMillis = threadWaitTimeMillis;
        this.callBack = callBack;
        this.isSentinelResetEnabled = isSentinelResetEnabled;
    }

    @Override
    public void run() {
        running.set(true);
        while (running.get()) {
            jedis = new Jedis(host, port);
            try {
                jedis.subscribe(new JedisPubSub() {
                                    @Override
                                    public void onMessage(String channel, String message) {
                                        processMessage(channel, message);
                                    }
                                }, POSITIVE_SDOWN, NEGATIVE_SDOWN, POSITIVE_RESET_MASTER, POSITIVE_SWITCH_MASTER, POSTIVE_ODOWN,
                        NEGATIVE_ODOWN, POSTIVE_SLAVE);
            } catch (JedisConnectionException e) {
                log.error("Lost connection to Sentinel at {} : {} due to {}", host, port, e);
                if (running.get()) {
                    try {
                        Thread.sleep(threadWaitTimeMillis);
                        if (!RedisUtils.isHostReachable(jedis))
                            shutdown();
                    } catch (InterruptedException e1) {
                        log.error("Sleep interrupted: ", e1);
                    }
                }
            } finally {
                jedis.close();
            }
        }
    }

    private void processMessage(String channel, String message) {
        try {
            log.info("Channel Message {} {}", message, masterName);
            String mName = extractMasterName(channel, message);
            String role = fetchRole(channel, message);

            // This is done to avoid initialization of Pool by all the sentinels simultaneously
            synchronized (this) {
                if (masterName.equals(mName)) {
                    if (isSentinelResetEnabled.apply(null)) {
                        if (!(channel.contains("reset-master") || channel.contains("slave")) && role.equals("slave")) {
                            sentinelReset();
                        }
                        if (channel.contains("switch-master") && callBack != null) {
                            callBack.accept(null);
                        }
                    } else if (callBack != null) {
                        callBack.accept(null);
                    }
                }

            }
        } catch (Exception e) {
            log.error("error while closing the internal Pool", e);
        }
    }

    private String extractMasterName(String channel, String message) {
        // Sample slave message: +sdown slave Ip:Port Ip Port @ masterName MasterIp MasterPort
        String[] slaveMsg = message.split(" ");
        switch (channel) {
            case POSITIVE_SWITCH_MASTER:
                return slaveMsg[0];
            case POSTIVE_SLAVE:
            case POSITIVE_SDOWN:
            case NEGATIVE_SDOWN:
            case POSTIVE_ODOWN:
            case NEGATIVE_ODOWN:
            case POSITIVE_RESET_MASTER:
                return slaveMsg[slaveMsg.length - 3];
            default:
                return null;
        }
    }

    private String fetchRole(String channel, String message) {
        String[] slaveMsg = message.split(" ");
        switch (channel) {
            case POSTIVE_SLAVE:
            case POSITIVE_SDOWN:
            case NEGATIVE_SDOWN:
            case POSTIVE_ODOWN:
            case NEGATIVE_ODOWN:
                return slaveMsg[0];
            default:
                return "master";
        }
    }

    public void shutdown() {
        log.info("Shutting down listener on " + host + ":" + port);
        running.set(false);
        // disconnecting from sentinel when the pool is destroyed
        jedis.disconnect();
    }

    private void sentinelReset() {
        long now = System.currentTimeMillis();
        if ((now - lastResetDelay.get()) > 15000) {
            lastResetDelay.set(now);
            try (Jedis jedis = new Jedis(host, port)) {
                log.info("Calling sentinel reset to refresh master-slave count");
                jedis.sentinelReset(masterName);
            } catch (Exception e) {
                log.error("Sentinel slave reset issue {}:{}", host, port, e);
            }
        }
    }
}
