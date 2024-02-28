package com.vortex.common.concurrent;

import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/*
*   this class deals when there is a lot of concurrency
*   like when we have a lot of events (commit, snapshot, rollback)
*   this class allows to pause, resume or prepare in/out threads
* */

public class PausableScheduledThreadPool extends ScheduledThreadPoolExecutor {

    private static final Logger LOG = Log.logger(PausableScheduledThreadPool.class);

    private volatile boolean paused = false;

    public PausableScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    public synchronized void pauseSchedule() {
        this.paused = true;
        LOG.info("PausableScheduledThreadPool was paused");
    }

    public synchronized void resumeSchedule() {
        this.paused = false;
        this.notifyAll();
        LOG.info("PausableScheduledthreadPool was resumed");
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        synchronized (this) {
            while (this.paused) {
                try {
                    this.wait();
                } catch (InterruptedException e ) {
                    LOG.warn("PausableScheduledThreadPool was interrumpted");
                }
            }
        }
    }

    @Override
    public void shutdown() {
        if (this.paused) {
            this.resumeSchedule();
        }

        super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        if (this.paused) {
            this.resumeSchedule();
        }

        return super.shutdownNow();
    }
}
