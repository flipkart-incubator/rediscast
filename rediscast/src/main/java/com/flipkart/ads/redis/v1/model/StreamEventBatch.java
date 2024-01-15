package com.flipkart.ads.redis.v1.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import redis.clients.jedis.StreamEntryID;

import java.util.ArrayList;
import java.util.List;

@Getter
public class StreamEventBatch<K, V> {
    private final List<StreamEvent<K, V>> streamEventsData = new ArrayList<>();
    @Setter
    private StreamEntryID maxStreamId;

    public StreamEventBatch(List<StreamEvent<K, V>> streamEventsData,
                            StreamEntryID maxStreamId) {
        if (CollectionUtils.isNotEmpty(streamEventsData)) {
            this.streamEventsData.addAll(streamEventsData);
        }
        this.maxStreamId = maxStreamId;
    }
}
