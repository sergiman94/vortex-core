
package com.vortex.vortexdb.job.system;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.vortexdb.job.EphemeralJob;
import com.vortex.vortexdb.job.EphemeralJobBuilder;
import com.vortex.vortexdb.job.system.JobCounters.JobCounter;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.structure.VortexIndex;
import com.vortex.vortexdb.task.VortexTask;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

public abstract class DeleteExpiredJob<T> extends EphemeralJob<T> {

    protected static final Logger LOG = Log.logger(DeleteExpiredJob.class);

    private static final int MAX_JOBS = 1000;
    protected static final JobCounters JOB_COUNTERS = new JobCounters();

    public static <V> void asyncDeleteExpiredObject(Vortex graph, V object) {
        E.checkArgumentNotNull(object, "The object can't be null");
        JobCounters.JobCounter jobCounter = JOB_COUNTERS.jobCounter(graph);
        if (!jobCounter.addAndTriggerDelete(object)) {
            return;
        }
        if (jobCounter.jobs() >= MAX_JOBS) {
            LOG.debug("Pending delete expired objects jobs size {} has " +
                      "reached the limit {}, abandon {}",
                      jobCounter.jobs(), MAX_JOBS, object);
            return;
        }
        jobCounter.increment();
        EphemeralJob<V> job = newDeleteExpiredElementJob(jobCounter, object);
        jobCounter.clear(object);
        VortexTask<?> task;
        try {
            task = EphemeralJobBuilder.<V>of(graph)
                                      .name("delete_expired_object")
                                      .job(job)
                                      .schedule();
        } catch (Throwable e) {
            jobCounter.decrement();
            if (e.getMessage().contains("Pending tasks size") &&
                e.getMessage().contains("has exceeded the max limit")) {
                // Reach tasks limit, just ignore it
                return;
            }
            throw e;
        }
        /*
         * If TASK_SYNC_DELETION is true, wait async thread done before
         * continue. This is used when running tests.
         */
        if (graph.option(CoreOptions.TASK_SYNC_DELETION)) {
            task.syncWait();
        }
    }

    public static <V> EphemeralJob<V> newDeleteExpiredElementJob(
                                      JobCounter jobCounter, V object) {
        if (object instanceof VortexElement) {
            return new DeleteExpiredElementJob<>(jobCounter.elements());
        } else {
            assert object instanceof VortexIndex;
            return new DeleteExpiredIndexJob<>(jobCounter.indexes());
        }
    }
}
