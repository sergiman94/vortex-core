
package com.vortex.common.concurrent;

import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

public class AtomicLock {

    private static final Logger LOG = Log.logger(LockManager.class);

    private String name;
    private AtomicReference<Thread> sign;

    public AtomicLock(String name) {
        this.name = name;
        this.sign = new AtomicReference<>();
    }

    public boolean tryLock() {
        Thread current = Thread.currentThread();
        return this.sign.compareAndSet(null, current);
    }

    public void unlock() {
        if (this.sign.get() == null) {
            return;
        }
        Thread current = Thread.currentThread();
        if (!this.sign.compareAndSet(current, null)) {
            throw new RuntimeException(String.format(
                      "Thread '%s' trying to unlock '%s' " +
                      "which is held by other threads now.",
                      current.getName(), this.name));
        }
    }

    public boolean lock(int retries) {
        // The interval between retries is exponential growth, most wait
        // interval is 2^(retries-1)s. If retries=0, don't retry.
        if (retries < 0 || retries > 10) {
            throw new IllegalArgumentException(String.format(
                      "Locking retry times should be in [0, 10], but got %d",
                      retries));
        }

        boolean isLocked = false;
        try {
            for (int i = 0; !(isLocked = this.tryLock()) && i < retries; i++) {
                Thread.sleep(1000 * (1L << i));
            }
        } catch (InterruptedException ignored) {
            LOG.info("Thread sleep is interrupted.");
        }
        return isLocked;
    }

    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }
}
