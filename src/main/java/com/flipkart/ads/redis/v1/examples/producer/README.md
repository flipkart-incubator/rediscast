## Getting Started
For each change event sent via redis client, library stores the value of the event as normal Redis key value where key is `<mapname>_<key>` and value will be `RedisEntity` object. Ex: if map name is `sampleMapOne` and key is `1234`, they the redis key for this entity is `sampleMapOne_1234`.
Along with this, the key of the event will be stored in redis set separately. This redis set is used to fetch all the keys present in a map which is required for fetch the entire data in consumer.
key name of the set in the redis is `<mapname>_ids` ex: if map name is `sampleMapOne`, then the key of the set is `sampleMapOne_ids`.

[//]: # (To initialise the producer, first the required dependent objects needs to be created. For dropwizard guice, a module is provided in the library which takes few configs from client to get initialised. )

[//]: # (For other frameworks, we can instantiate the required objects in a similar way.)

The events can be produced by using `GenericRedisWriteOnlyClient` class. Example producer is given [here](SampleProducer.java)

### Steps for creating producer
* Implement `RedisMap` and define the custom redis map. Example can be found [here](../models/SampleRedisMap.java). Here map can be thought of as a namespace or topic where same entity events are stored.
* Since library stores the entities as key value strings in redis, a transformer needs to be defined for each map which has the logic of converting POJO object to string. Example is given [here](../transformers/SampleEntityTransformer.java)
* Redis config needs to be defined.
  * Redis infra details. Ex: sentinel connection details, timeout etc
  * Mapping between RedisMap and its corresponding transformer
  * Mapping between RedisMap and Stream config like how many events to retain in redis.

All these can be defined in a client guice module. Sample producer module is given [here](modules/ProducerModule.java)
