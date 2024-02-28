
package com.vortex.vortexdb.util;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class is used for fixed watch-window to rate limit request
 * Now just simplify for performance, don't need lock stop watch
 *
 * Note: This class is not thread safe
 * TODO: Move to common module
 * */
public class FixedWatchWindowRateLimiter implements RateLimiter {

    private final LongAdder count;
    private final Stopwatch watch;
    private final int limit;

    public FixedWatchWindowRateLimiter(int limitPerSecond) {
        this.limit = limitPerSecond;
        this.watch = Stopwatch.createStarted();
        this.count = new LongAdder();
    }

    @Override
    public boolean tryAcquire() {
        if (count.intValue() < limit) {
            count.increment();
            return true;
        }

        // Reset only if 1000ms elapsed
        if (watch.elapsed(TimeUnit.MILLISECONDS) >= RESET_PERIOD) {
            count.reset();
            watch.reset();
            count.increment();
            return true;
        }
        return false;
    }
}
