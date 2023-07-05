package com.flipkart.ads.redis.v1.utils;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.*;

@Slf4j
public class RedisUtils {
    public static boolean isHostReachable(Jedis jedis) {
        try {
            return jedis.ping().equals("PONG");
        } catch (Exception e) {
            log.error("Could not reach slave due to : {} ", e.getMessage());
            return false;
        }
    }

    public static Map.Entry<String, String> toMasterAndPassword(String master) {
        String[] masterNameNPassword = master.split(":::");
        String masterName = masterNameNPassword[0];
        String masterPassword = masterNameNPassword.length > 1 ? masterNameNPassword[1] : null;
        return new AbstractMap.SimpleEntry<>(masterName, masterPassword);
    }


    public static HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
        return new HostAndPort(host, port);
    }

    public static HostAndPort toHostAndPort(String hostNPort) {
        return toHostAndPort(Arrays.asList(hostNPort.split(":")));
    }
}
