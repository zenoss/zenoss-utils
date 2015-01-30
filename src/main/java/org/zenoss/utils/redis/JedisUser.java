package org.zenoss.utils.redis;

import redis.clients.jedis.Jedis;

public interface JedisUser<T> {
    T use(Jedis jedis) throws RedisTransactionCollision;
}
