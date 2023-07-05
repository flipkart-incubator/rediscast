package com.flipkart.ads.redis.v1.stream;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.model.StreamEvent;
import com.flipkart.ads.redis.v1.model.StreamEventBatch;
import com.flipkart.ads.redis.v1.client.RedisReadOnlyClient;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class RedisStreamListenerImpl implements RedisStreamListener {
    private final RedisReadOnlyClient redisReadOnlyClient;
    private final String mapName;
    private final boolean metaWithSameSlaveConnection;
    private final Timer batchReadDelay;
    private final Meter batchSizeCount;
    private StreamEntryID streamIdTracker;


    @Inject
    public RedisStreamListenerImpl(MetricRegistry metricRegistry, RedisReadOnlyClient redisReadOnlyClient,
                                   String mapName, boolean metaWithSameSlaveConnection) {
        this.mapName = mapName;
        this.redisReadOnlyClient = redisReadOnlyClient;
        this.streamIdTracker = new StreamEntryID(System.currentTimeMillis() - 500, 0L);
        this.metaWithSameSlaveConnection = metaWithSameSlaveConnection;
        this.batchReadDelay = metricRegistry.timer(MetricRegistry.name(RedisStreamListenerImpl.class, "batchReadDelay", mapName));
        this.batchSizeCount = metricRegistry.meter(MetricRegistry.name(RedisStreamListenerImpl.class, "batchSizeCount", mapName));
    }

    @Override
    public String getId() {
        return mapName;
    }

    @Override
    public StreamEntryID getLatestOffset() {
        return streamIdTracker;
    }

    @Override
    public List<StreamEvent<String, Object>> nextBatch() throws RedisDataStoreException, InterruptedException {
        return metaWithSameSlaveConnection ? nextBatchMetaWithinSameResource() : nextBatchMetaWithNewResource();
    }

    private List<StreamEvent<String, Object>> nextBatchMetaWithinSameResource() throws RedisDataStoreException, InterruptedException {
        StreamEventBatch<String, Object> batchData = redisReadOnlyClient.nextBatchInStreamEventBatch(mapName, streamIdTracker);
        long currentEpoc = streamIdTracker.getTime();
        long newEpoch = batchData.getMaxStreamId().getTime();
        batchReadDelay.update(currentEpoc - newEpoch, TimeUnit.MILLISECONDS);
        batchSizeCount.mark(batchData.getStreamEventsData().size());
        streamIdTracker = batchData.getMaxStreamId();
        return batchData.getStreamEventsData();
    }

    // Keeping function for easy switch
    private List<StreamEvent<String, Object>> nextBatchMetaWithNewResource() throws RedisDataStoreException, InterruptedException {
        List<StreamEntry> streamEntries = redisReadOnlyClient.nextBatch(mapName, streamIdTracker);
        if (CollectionUtils.isNotEmpty(streamEntries)) {
            return streamEntries.stream().map(this::getStreamEvent).filter(Objects::nonNull).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    private StreamEvent<String, Object> getStreamEvent(StreamEntry entry) {
        if (entry != null) {
            streamIdTracker = entry.getID();
            if (MapUtils.isNotEmpty(entry.getFields())) {
                return redisReadOnlyClient.getStreamEvent(entry.getFields(), mapName, true);
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
