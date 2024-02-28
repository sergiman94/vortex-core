
package com.vortex.vortexdb.job;

import com.vortex.vortexdb.task.TaskCallable.SysTaskCallable;

public abstract class EphemeralJob<V> extends SysTaskCallable<V> {

    public abstract String type();

    public abstract V execute() throws Exception;

    @Override
    public V call() throws Exception {
        return this.execute();
    }
}
