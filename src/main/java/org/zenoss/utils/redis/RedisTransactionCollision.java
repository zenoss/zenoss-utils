package org.zenoss.utils.redis;

public class RedisTransactionCollision extends RuntimeException {
    public RedisTransactionCollision(String msg) {
        super(msg);
    }
}
