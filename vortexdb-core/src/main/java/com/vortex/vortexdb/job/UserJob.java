
package com.vortex.vortexdb.job;

import com.vortex.vortexdb.task.TaskCallable;

public abstract class UserJob<V> extends TaskCallable<V> implements Job<V> {

    @Override
    public V call() throws Exception {
        this.save();
        return this.execute();
    }

    @Override
    protected void done() {
        try {
            this.save();
        } finally {
            super.done();
        }
    }

    @Override
    protected void cancelled() {
        this.save();
    }
}
