
package com.vortex.vortexdb.util;

import com.vortex.common.util.Log;
import org.slf4j.Logger;

// TODO: Move to common module (concurrent package)
public interface RateLimiter {

    public final Logger LOG = Log.logger(RateLimiter.class);

    public final long RESET_PERIOD = 1000L;

    /**
     * Acquires one permit from RateLimiter if it can be acquired immediately
     * without delay.
     */
    public boolean tryAcquire();

    /**
     * Create a RateLimiter with specified rate, to keep compatible with
     * Guava's RateLimiter (use double now)
     *
     * @param ratePerSecond the rate of the returned RateLimiter, measured in
     *                      how many permits become available per second
     *
     * TODO: refactor it to make method unchangeable
     */
    public static RateLimiter create(double ratePerSecond) {
        return new com.vortex.vortexdb.util.FixedTimerWindowRateLimiter((int) ratePerSecond);
    }
}
