package com.flipkart.ads.redis.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SampleEntity {
    private String entityType;
    private String entityValue;
}
