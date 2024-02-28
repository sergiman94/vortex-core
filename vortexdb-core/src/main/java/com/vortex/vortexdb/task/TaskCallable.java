
package com.vortex.vortexdb.task;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.VortexParams;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.slf4j.Logger;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.Callable;

public abstract class TaskCallable<V> implements Callable<V> {

    private static final Logger LOG = Log.logger(VortexTask.class);

    private static final String ERROR_COMMIT = "Failed to commit changes: ";
    private static final Set<String> ERROR_MESSAGES = ImmutableSet.of(
            /*
             * "The max length of bytes is" exception message occurs when
             * task input size exceeds TASK_INPUT_SIZE_LIMIT or task result size
             * exceeds TASK_RESULT_SIZE_LIMIT
             */
            "The max length of bytes is",
            /*
             * "Batch too large" exception message occurs when using
             * cassandra store and task input size is in
             * [batch_size_fail_threshold_in_kb, TASK_INPUT_SIZE_LIMIT) or
             * task result size is in
             * [batch_size_fail_threshold_in_kb, TASK_RESULT_SIZE_LIMIT)
             */
            "Batch too large"
    );

    private VortexTask<V> task = null;
    private Vortex graph = null;

    private volatile long lastSaveTime = System.currentTimeMillis();
    private volatile long saveInterval = 1000 * 30;

    public TaskCallable() {
        // pass
    }

    protected void done() {
        this.closeTx();
    }

    protected void cancelled() {
        // Do nothing, subclasses may override this method
    }

    protected void closeTx() {
        Transaction tx = this.graph().tx();
        if (tx.isOpen()) {
            tx.close();
        }
    }

    public void setMinSaveInterval(long seconds) {
        E.checkArgument(seconds > 0,
                        "Must set interval > 0, but got '%s'", seconds);
        this.saveInterval = seconds * 1000L;
    }

    public void updateProgress(int progress) {
        VortexTask<V> task = this.task();
        task.progress(progress);

        long elapse = System.currentTimeMillis() - this.lastSaveTime;
        if (elapse > this.saveInterval) {
            this.save();
            this.lastSaveTime = System.currentTimeMillis();
        }
    }

    public int progress() {
        VortexTask<V> task = this.task();
        return task.progress();
    }

    protected void save() {
        VortexTask<V> task = this.task();
        task.updateTime(new Date());
        try {
            this.graph().taskScheduler().save(task);
        } catch (Throwable e) {
            if (task.completed()) {
                /*
                 * Failed to save task and the status is stable(can't be update)
                 * just log the task, and try again.
                 */
                LOG.error("Failed to save task with error \"{}\": {}",
                          e, task.asMap(false));
                String message = e.getMessage();
                if (message.contains(ERROR_COMMIT) && needSaveWithEx(message)) {
                    task.failToSave(e);
                    this.graph().taskScheduler().save(task);
                    return;
                }
            }
            throw e;
        }
    }

    protected void graph(Vortex graph) {
        this.graph = graph;
    }

    public Vortex graph() {
        E.checkState(this.graph != null,
                     "Can't call graph() before scheduling task");
        return this.graph;
    }

    protected void task(VortexTask<V> task) {
        this.task = task;
    }

    public VortexTask<V> task() {
        E.checkState(this.task != null,
                     "Can't call task() before scheduling task");
        return this.task;
    }

    @SuppressWarnings("unchecked")
    public static <V> TaskCallable<V> fromClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (TaskCallable<V>) clazz.newInstance();
        } catch (Exception e) {
            throw new VortexException("Failed to load task: %s", e, className);
        }
    }

    private static boolean needSaveWithEx(String message) {
        for (String error : ERROR_MESSAGES) {
            if (message.contains(error)) {
                return true;
            }
        }
        return false;
    }

    public static <V> TaskCallable<V> empty(Exception e) {
        return new TaskCallable<V>() {
            @Override
            public V call() throws Exception {
                throw e;
            }
        };
    }

    public static abstract class SysTaskCallable<V> extends TaskCallable<V> {

        private VortexParams params = null;

        protected void params(VortexParams params) {
            this.params = params;
        }

        protected VortexParams params() {
            E.checkState(this.params != null,
                         "Can't call scheduler() before scheduling task");
            return this.params;
        }
    }
}
