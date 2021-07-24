package com.example.redislimiter.Limiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class Limiter {

    private String key;

    private final JedisPool jedisPool;

    private final JedisCluster jedisCluster;

    private RedisRateLimiter redisRateLimiter;

    public RedisRateLimiter getRedisRateLimiter() {
        return redisRateLimiter;
    }

    private static final ConcurrentHashMap<String, Limiter> hashMap = new ConcurrentHashMap<>();

    public Limiter(String key, JedisPool jedisPool, JedisCluster jedisCluster, boolean isInit, double permitsPerSecond, Integer maxBurstSecond) throws Exception {
        this.key = key;
        redisRateLimiter = new RedisRateLimiter(permitsPerSecond, maxBurstSecond);
        this.jedisPool = jedisPool;
        this.jedisCluster = jedisCluster;
        if (isInit) {
            init();
        }
    }

    private void init() throws Exception {
        if (null != jedisPool) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.hmset(this.key, hashMap.get(this.key).getRedisRateLimiter().toMap());
            } catch (Exception e) {
                throw new Exception(e);
            }
        } else {
            try  {
                jedisCluster.hmset(this.key, hashMap.get(this.key).getRedisRateLimiter().toMap());
            } catch (Exception e) {
                throw new Exception(e);
            }
        }
    }

    public static Limiter create(String key, JedisPool jedisPool, JedisCluster jedisCluster, boolean isInit, double permitsPerSecond, Integer maxBurstSecond) throws Exception {
        if (hashMap.get(key) == null) {
            Limiter limiter = new Limiter(key, jedisPool, jedisCluster, isInit, permitsPerSecond, maxBurstSecond);
            hashMap.put(key, limiter);
        }
        return hashMap.get(key);
    }
    

}
