
package com.vortex.vortexdb.task;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public interface TaskScheduler {

    public Vortex graph();

    public int pendingTasks();

    public <V> void restoreTasks();

    public <V> Future<?> schedule(VortexTask<V> task);

    public <V> void cancel(VortexTask<V> task);

    public <V> void save(VortexTask<V> task);

    public <V> VortexTask<V> delete(Id id);

    public <V> VortexTask<V> task(Id id);
    public <V> Iterator<VortexTask<V>> tasks(List<Id> ids);
    public <V> Iterator<VortexTask<V>> tasks(TaskStatus status,
                                           long limit, String page);

    public boolean close();

    public <V> VortexTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                  throws TimeoutException;

    public <V> VortexTask<V> waitUntilTaskCompleted(Id id)
                                                  throws TimeoutException;

    public void waitUntilAllTasksCompleted(long seconds)
                                           throws TimeoutException;

    public void checkRequirement(String op);
}
