
package com.vortex.vortexdb.job;

import com.vortex.vortexdb.task.TaskCallable.SysTaskCallable;

public abstract class SysJob<V> extends SysTaskCallable<V> implements Job<V> {

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
