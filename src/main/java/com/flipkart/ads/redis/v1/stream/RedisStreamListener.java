package com.flipkart.ads.redis.v1.stream;

import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreException;
import com.flipkart.ads.redis.v1.model.StreamEvent;
import redis.clients.jedis.StreamEntryID;

import java.io.Closeable;
import java.util.List;

public interface RedisStreamListener extends Closeable {
    String getId();

    StreamEntryID getLatestOffset();

    List<StreamEvent<String, Object>> nextBatch() throws RedisDataStoreException, InterruptedException;
}
