# rediscast
Rediscast is an In-memory data grid similar to Hazelcast, that synchronizes Entity changes in Redis server with all cluster nodes (JVM instances) within a short SLA. It uses Redis Streams for change data propagation.


## Intro

rediscast library uses redis store and redis streams to propagate the events. Events will be stored as key value pairs in redis and an event will be generated and pushed to redis stream from the producer.
Consumer will periodically poll redis streams for new events. If there are new events, library will do a key value lookup to fetch entity corresponding to the event and notifies the configured listener.


## Getting Started
For starting producer, refer to this [doc](src/main/java/com/flipkart/ads/redis/v1/examples/producer/README.md)

For starting consumer, refer to this [doc](src/main/java/com/flipkart/ads/redis/v1/examples/consumer/README.md)
