package com.example.redislimiter.Limiter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

public class RedisRateLimiter implements Serializable {

    private double storedPermits;

    private double maxPermits;

    private double stableIntervalMicros;

    private long nextFreeTicketMicros;

    private double maxBurstSecond;

//    @Override
//    public String toString() {
//        return
//    }
    public Map<String, String> toMap() {
        Map<String, String> limiter = new HashMap<>();
        limiter.put("storedPermits", Double.toString(maxPermits));
        limiter.put("maxPermits", Double.toString(maxPermits));
        limiter.put("stableIntervalMicros", Double.toString(stableIntervalMicros));
        limiter.put("nextFreeTicketMicros", "0");
        return limiter;
    }

    public RedisRateLimiter(double permitsPerSecond, Integer maxBurstSecond) {
        if (null == maxBurstSecond) {
            maxBurstSecond = 60;
        }
        this.maxBurstSecond = maxBurstSecond;
        this.maxPermits = permitsPerSecond * maxBurstSecond;
        this.storedPermits = permitsPerSecond;
        this.stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;
        this.nextFreeTicketMicros = 0L;
    }

    private void reSync(long nowMicros) {
        if (nowMicros > nextFreeTicketMicros) {
            double newPermits = (nowMicros - nextFreeTicketMicros) / stableIntervalMicros;
            storedPermits = Math.min(maxPermits, storedPermits + newPermits);
            nextFreeTicketMicros = nowMicros;
        }
    }

    private long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
        reSync(nowMicros);
        // 允许赊账
        long returnValue = nextFreeTicketMicros;
        double storedPermitsToSpend = Math.min(requiredPermits, this.storedPermits);
        double freshPermits = requiredPermits - storedPermitsToSpend;
        long waitMicros = (long) (freshPermits * stableIntervalMicros);
        // 加上还账时间
        this.nextFreeTicketMicros = this.nextFreeTicketMicros + waitMicros;
        this.storedPermits -= storedPermitsToSpend;
        return returnValue;
    }

    private static void checkPermits(int permits) {
        checkArgument(permits > 0, "Requested permits (%s) must be positive", permits);
    }

    private static void checkArgument(boolean b, String errorMessageTemplate, int p1) {
        if (!b) {
            throw new IllegalArgumentException(errorMessageTemplate);
        }
    }

    public double acquire(int permits) throws InterruptedException {
        long microsToWait = reserve(permits);
        Thread.sleep(microsToWait);
        return 1.0 * microsToWait / SECONDS.toMicros(1L);
    }

    final long reserve(int permits) {
        checkPermits(permits);
        return reserveEarliestAvailable(permits, System.currentTimeMillis());
    }

    public RedisRateLimiter create(double permitsPerSecond, Integer maxBurstSecond) {
        RedisRateLimiter rateLimiter = new RedisRateLimiter(permitsPerSecond, maxBurstSecond);
        return rateLimiter;
    }

}
