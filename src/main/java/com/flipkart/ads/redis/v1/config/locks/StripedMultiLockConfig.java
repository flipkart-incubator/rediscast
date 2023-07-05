package com.flipkart.ads.redis.v1.config.locks;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Getter;

@Getter
public class StripedMultiLockConfig extends LockConfig {
    private int stripes = 500;

    public StripedMultiLockConfig() {
        super(LockType.STRIPED_MULTI_LOCK);
    }

    @JsonCreator
    public StripedMultiLockConfig(int stripes) {
        super(LockType.STRIPED_MULTI_LOCK);
        this.stripes = stripes;
    }
}
