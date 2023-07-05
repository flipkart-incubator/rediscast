package com.flipkart.ads.redis.v1.ingestion;

import com.flipkart.ads.redis.v1.ingestion.stategy.RedisInitialisationStrategy;
import com.flipkart.ads.redis.v1.model.RedisMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Singleton
public class RedisInitialiser {
    //IMP:Need to singleton provider
    private final Provider<Map<RedisMap, Object>> redisCachesProvider;
    private final RedisInitialisationStrategy redisInitialisationStrategy;
    private final List<RedisMap> maps;
    private final AtomicBoolean isInitialised = new AtomicBoolean(false);

    @Inject
    public RedisInitialiser(@Named("redis_cache_map") Provider<Map<RedisMap, Object>> redisCachesProvider, RedisInitialisationStrategy redisInitialisationStrategy,
                            List<RedisMap> maps) {
        this.redisCachesProvider = redisCachesProvider;
        this.maps = maps;
        this.redisInitialisationStrategy = redisInitialisationStrategy;
    }

    public synchronized void initialise(Boolean isLeader) {

        log.info("Initialising ingestion with new leader changes: {}", isLeader);
        destroyIngestionMaps();
        initialiseRedisCaches(isLeader);
    }

    //Adding synchronised to make this thread safe
    private void initialiseRedisCaches(Boolean isLeader) {
        //If not initialised
        if (!isInitialised.get()) {
            try {
                //Made changes to allow banner group initialisation before banner initialisation
                Map<RedisMap, Object> redisCaches = redisCachesProvider.get();
                for (RedisMap map : maps) {
                    Object cacheObject = redisCaches.get(map);
                    log.info("Initialising map : {}", map);
                    //Put back object
                    // Need to make this put call only for maps that need to maintain a local cache
                    // The implementation will take care whether we need to populate the cache or not
                    redisInitialisationStrategy.putInCache(map, cacheObject);
                    redisInitialisationStrategy.initialise(isLeader, cacheObject);
                }
                log.info("Full load completed.");
                isInitialised.set(true);
            } catch (Exception e) {
                log.error("Error while init call of ingestion caches: ", e);
                throw new RuntimeException(e);
            }
        }
    }

    //Adding synchronised to make this thread safe
    private void destroyIngestionMaps() {

        if (isInitialised.get()) {

            Map<RedisMap, Object> ingestionCaches = redisCachesProvider.get();

            for (RedisMap map : maps) {
                Object cacheObject = ingestionCaches.get(map);
                try {
                    redisInitialisationStrategy.destroy(cacheObject);
                } catch (Exception ex) {
                    log.error("Exception during destroy call for Ingestion map : {} ", map, ex);
                    throw new RuntimeException(ex);
                }
            }
        }

        //Prime it for initialisation if it becomes leader later on
        isInitialised.set(false);
    }
}
