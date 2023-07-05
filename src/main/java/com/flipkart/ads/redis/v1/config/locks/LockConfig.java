package com.flipkart.ads.redis.v1.config.locks;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        visible = true
)
@JsonSubTypes({@JsonSubTypes.Type(
        name = "STRIPED_MULTI_LOCK",
        value = StripedMultiLockConfig.class
), @JsonSubTypes.Type(
        name = "MAP_BASED_MULTI_LOCK",
        value = LockConfig.class
)})
@JsonIgnoreProperties(
        ignoreUnknown = true
)
@AllArgsConstructor
public class LockConfig {
    LockType type;

    public enum LockType {
        STRIPED_MULTI_LOCK,
        MAP_BASED_MULTI_LOCK;
    }
}
