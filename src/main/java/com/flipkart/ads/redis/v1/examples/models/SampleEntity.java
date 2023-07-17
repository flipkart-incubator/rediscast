package com.flipkart.ads.redis.v1.examples.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SampleEntity {
    private String entityType;
    private String entityValue;
}
