
package com.vortex.vortexdb.util;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class is used for fixed window to limit request per second
 * The different with stopwatch is to use timer for reducing count times
 *
 * TODO: Move to common module
 */
public class FixedTimerWindowRateLimiter implements RateLimiter {

    private final Timer timer;
    private final LongAdder count;
    private final int limit;

    public FixedTimerWindowRateLimiter(int limitPerSecond) {
        this.timer = new Timer("RateAuditLog", true);
        this.count = new LongAdder();
        this.limit = limitPerSecond;
        // Count will be reset if hit limit (run once per 1000ms)
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (count.intValue() >= limit) {
                    count.reset();
                }
            }
        }, 0L, RESET_PERIOD);
    }

    @Override
    public boolean tryAcquire() {
        if (count.intValue() >= limit) {
            return false;
        }

        count.increment();
        return true;
    }
}
