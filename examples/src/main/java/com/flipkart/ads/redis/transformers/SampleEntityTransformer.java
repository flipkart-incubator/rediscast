package com.flipkart.ads.redis.transformers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ads.redis.models.SampleEntity;
import com.flipkart.ads.redis.v1.exceptions.RedisDataTransformerException;
import com.flipkart.ads.redis.v1.transformers.RedisTransformer;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class SampleEntityTransformer implements RedisTransformer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String apply(Object data) throws RedisDataTransformerException {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (Exception exp) {
            throw new RedisDataTransformerException("Error in transforming the sample entity data to string", exp);
        }
    }

    @Override
    public Object revert(String data) throws RedisDataTransformerException {
        try {
            return objectMapper.readValue(data, SampleEntity.class);
        } catch (Exception exp) {
            throw new RedisDataTransformerException("Error in transforming the redis String to sample entity", exp);
        }
    }
}
