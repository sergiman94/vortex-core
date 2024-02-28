
package com.vortex.common.concurrent;


import com.vortex.common.util.E;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BarrierEvent {

    private final Lock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();
    private volatile boolean signaled = false;

    /**
     * Wait forever until the signal is received.
     * @throws InterruptedException if interrupted.
     */
    public void await() throws InterruptedException {
        this.lock.lock();
        try {
            while (!this.signaled) {
                this.cond.await();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Wait specified time in milliseconds.
     * @param timeout: the time in millisecond to wait.
     * @return true if signal is received, false if time out.
     * @throws InterruptedException if interrupted.
     */
    public boolean await(long timeout) throws InterruptedException {
        E.checkArgument(timeout >= 0L,
                        "The time must be >= 0, but got '%d'.",
                        timeout);
        long deadline = System.currentTimeMillis() + timeout;
        this.lock.lock();
        try {
            while (!this.signaled) {
                timeout = deadline - System.currentTimeMillis();
                if (timeout > 0) {
                    this.cond.await(timeout, TimeUnit.MILLISECONDS);
                }
                if (System.currentTimeMillis() >= deadline) {
                    return this.signaled;
                }
            }
        } finally {
            this.lock.unlock();
        }
        return true;
    }

    public void reset() {
        this.lock.lock();
        try {
            this.signaled = false;
        } finally {
            this.lock.unlock();
        }
    }

    public void signal() {
        this.lock.lock();
        try {
            this.signaled = true;
            this.cond.signal();
        } finally {
            this.lock.unlock();
        }
    }

    public void signalAll() {
        this.lock.lock();
        try {
            this.signaled = true;
            this.cond.signalAll();
        } finally {
            this.lock.unlock();
        }
    }
}
