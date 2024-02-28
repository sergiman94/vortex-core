
package com.vortex.vortexdb.job;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.task.VortexTask;
import com.vortex.vortexdb.task.TaskScheduler;
import com.vortex.common.util.E;

public class EphemeralJobBuilder<V> {

    private final Vortex graph;

    private String name;
    private String input;
    private EphemeralJob<V> job;

    // Use negative task id for ephemeral task
    private static int ephemeralTaskId = -1;

    public static <T> EphemeralJobBuilder<T> of(final Vortex graph) {
        return new EphemeralJobBuilder<>(graph);
    }

    public EphemeralJobBuilder(final Vortex graph) {
        this.graph = graph;
    }

    public EphemeralJobBuilder<V> name(String name) {
        this.name = name;
        return this;
    }

    public EphemeralJobBuilder<V> input(String input) {
        this.input = input;
        return this;
    }

    public EphemeralJobBuilder<V> job(EphemeralJob<V> job) {
        this.job = job;
        return this;
    }

    public VortexTask<V> schedule() {
        E.checkArgumentNotNull(this.name, "Job name can't be null");
        E.checkArgumentNotNull(this.job, "Job can't be null");

        VortexTask<V> task = new VortexTask<>(this.genTaskId(), null, this.job);
        task.type(this.job.type());
        task.name(this.name);
        if (this.input != null) {
            task.input(this.input);
        }

        TaskScheduler scheduler = this.graph.taskScheduler();
        scheduler.schedule(task);

        return task;
    }

    private Id genTaskId() {
        if (ephemeralTaskId >= 0) {
            ephemeralTaskId = -1;
        }
        return IdGenerator.of(ephemeralTaskId--);
    }
}
