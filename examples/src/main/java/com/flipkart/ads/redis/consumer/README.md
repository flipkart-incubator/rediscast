## Getting Started
In consumer, for each given map, a task will be scheduled in a executor service which polls for new events from redis streams periodically.
Once poller receives the events, it calls the client's custom defined listener to act on them. If localCopy is enabled for the map, it also stores the events in memory which can be accessed later.

Consumer can be started by using `RedisInitialiser` and the flow starts from this class. Sample consumer is given [here](SampleConsumer.java)

### Steps for creating Consumer
* Implement `RedisMap` and define the custom redis map. Example can be found [here](../models/SampleRedisMap.java). Here map can be thought of as a namespace or topic where same entity events are stored.
* Since library stores the entities as key value strings in redis, a transformer needs to be defined for each map which has the logic of converting String to POJO. Example is given [here](../transformers/SampleEntityTransformer.java). The same transformer can be used in producer and consumer
* If client wants to act on the events received, a custom listener should be defined which will be notfied if there are any new events. Sample lister is given [here](listener/SampleEntityListener.java)
* Dependencies required for initializing consumer
  * Redis infra details. Ex: sentinel connection details, timeout etc
  * Mapping between RedisMap and its corresponding transformer
  * Mapping between RedisMap and Stream config like how many events to retain in redis.
  * Mapping between RedisMap and Event processor config

All these can be defined in a client guice module. Sample consumer module is given [here](modules/ConsumerModule.java)
