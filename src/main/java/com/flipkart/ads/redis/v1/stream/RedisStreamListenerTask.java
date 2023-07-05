package com.flipkart.ads.redis.v1.stream;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.ads.redis.v1.event.RedisDataStoreChangePropagator;
import com.flipkart.ads.redis.v1.exceptions.RedisDataStoreChangePropagatorException;
import com.flipkart.ads.redis.v1.model.RedisEntity;
import com.flipkart.ads.redis.v1.model.StreamEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class RedisStreamListenerTask implements Runnable {
    private static final String IS_PARALLEL_PROCESSING_ON_STREAM_BATCH_ENABLED = "is_parallel_process_on_stream_batch_enabled";
    private final String mapName;
    private final RedisStreamListener changeListener;
    private final RedisDataStoreChangePropagator<String, Object> changePropagator;
    private final Timer batchProcessEvent;
    private final Meter batchProcessExceptionMeter;
    private final Meter batchProcessThrowableMeter;
    private final Meter reportEventExceptionMeter;
    private RedisStreamListenerTask.RedisStreamListenerTaskState state;

    public RedisStreamListenerTask(MetricRegistry metricRegistry, String mapName, RedisStreamListener changeListener, RedisDataStoreChangePropagator<String, Object> changePropagator) {
        this.mapName = mapName;
        this.state = RedisStreamListenerTask.RedisStreamListenerTaskState.INITIALIZING;
        this.changeListener = changeListener;
        this.changePropagator = changePropagator;

        this.batchProcessEvent = metricRegistry.timer(MetricRegistry.name(RedisStreamListenerTask.class, "processEvent", mapName));
        this.batchProcessExceptionMeter = metricRegistry.meter(MetricRegistry.name(RedisStreamListenerTask.class, "processEvent", "exception", mapName));
        this.batchProcessThrowableMeter = metricRegistry.meter(MetricRegistry.name(RedisStreamListenerTask.class, "processEvent", "error", mapName));
        this.reportEventExceptionMeter = metricRegistry.meter(MetricRegistry.name(RedisStreamListenerTask.class, "reportEvent", "exception", mapName));
    }

    @Override
    public void run() {
        if (RedisStreamListenerTask.RedisStreamListenerTaskState.RUNNING.equals(state)) {
            log.error("Executor already running for map {}", mapName);
            return;
        }
        state = RedisStreamListenerTask.RedisStreamListenerTaskState.RUNNING;
        try {
            if (Thread.interrupted()) {
                log.warn("Thread interrupted for changeListener mapName: {}, id: {}", mapName, changeListener.getId());
            }

            List<StreamEvent<String, Object>> changeEvents = changeListener.nextBatch();
            log.debug("Got batch for mapName: {}, found: {} records in batch.", mapName, changeEvents.size());

            try (Timer.Context context = batchProcessEvent.time()) {
                // Sort and map construction to remove duplicate events in a batch
                // Consider latest event for a entity at a batch level
                Map<String, List<StreamEvent<String, Object>>> uniqStreams = changeEvents.stream().collect(Collectors.groupingBy(RedisEntity::getKey));

                // TODO: Uday include it as a config
                Boolean isParallelProcessingOnStreamBatchEnabled = true;
                if (isParallelProcessingOnStreamBatchEnabled) {
                    log.info("Doing Parallel Stream on Data from Redis Stream Batch");
                    uniqStreams.values().parallelStream().forEach(e ->
                            e.stream().sorted(Comparator.comparingLong(RedisEntity::getEventTime))
                                    .collect(Collectors.toList()).forEach(r -> {
                                        try {
                                            reportEvent(r);
                                        } catch (Exception ex) {
                                            // @Todo Revisit the eating of all the exceptions
                                            log.error("Exception occurred in batch: eating the exception for mapName: {} , event: {}", mapName, e);
                                            batchProcessExceptionMeter.mark();
                                        }
                                    }));
                } else {
                    log.info("Doing Serial Stream on Data from Redis Stream Batch");
                    uniqStreams.values().stream().forEach(e ->
                            e.stream().sorted(Comparator.comparingLong(RedisEntity::getEventTime))
                                    .collect(Collectors.toList()).forEach(r -> {
                                        try {
                                            reportEvent(r);
                                        } catch (Exception ex) {
                                            // @Todo Revisit the eating of all the exceptions
                                            log.error("Exception occurred in batch: eating the exception for mapName: {} , event: {}", mapName, e);
                                            batchProcessExceptionMeter.mark();
                                        }
                                    }));
                }
                log.debug("Finished processing batch for mapName: {}, with {} records in batch.", mapName, changeEvents.size());
            }
        } catch (Exception e) {
            batchProcessExceptionMeter.mark();
            log.error("Exception occurred for mapName : {} ", mapName, e);
        } catch (Throwable e) {
            batchProcessThrowableMeter.mark();
            log.error("Error occurred for mapName : {} ", mapName, e);
        }
        log.debug("Finished RedisStreamListenerTask for mapName:{}", mapName);
        state = RedisStreamListenerTask.RedisStreamListenerTaskState.NOT_RUNNING;
    }

    private void reportEvent(StreamEvent<String, Object> streamEvent) {
        try {
            switch (streamEvent.getEventType()) {
                case ADD:
                    changePropagator.entryAdded(streamEvent);
                    break;
                case UPDATE:
                    changePropagator.entryUpdated(streamEvent);
                    break;
                case DELETE:
                    changePropagator.entryDeleted(streamEvent);
                    break;
            }
        } catch (RedisDataStoreChangePropagatorException e) {
            log.error("RedisDataStoreChangePropagatorException (eating up) could not notify to listener for mapName: {}",
                    mapName, e);
            reportEventExceptionMeter.mark();
        } catch (RuntimeException e) {
            log.error("Runtime (eating up):  Event Type: {}, mapName : {}, Key: {}, ttl :{} , event time:  {}, value: {} ",
                    streamEvent.getEventType(), mapName, streamEvent.getKey(), streamEvent.getTtl(),
                    streamEvent.getEventTime(), streamEvent.getValue(), e);
            reportEventExceptionMeter.mark();
        }
    }

    enum RedisStreamListenerTaskState {
        INITIALIZING,
        RUNNING,
        NOT_RUNNING
    }
}
