
package com.vortex.vortexdb.job;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.task.VortexTask;
import com.vortex.vortexdb.task.TaskCallable;
import com.vortex.vortexdb.task.TaskScheduler;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;

import java.util.Set;

public class JobBuilder<V> {

    private final Vortex graph;

    private String name;
    private String input;
    private Job<V> job;
    private Set<Id> dependencies;

    public static <T> JobBuilder<T> of(final Vortex graph) {
        return new JobBuilder<>(graph);
    }

    public JobBuilder(final Vortex graph) {
        this.graph = graph;
    }

    public JobBuilder<V> name(String name) {
        this.name = name;
        return this;
    }

    public JobBuilder<V> input(String input) {
        this.input = input;
        return this;
    }

    public JobBuilder<V> job(Job<V> job) {
        this.job = job;
        return this;
    }

    public JobBuilder<V> dependencies(Set<Id> dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public VortexTask<V> schedule() {
        E.checkArgumentNotNull(this.name, "Job name can't be null");
        E.checkArgumentNotNull(this.job, "Job callable can't be null");
        E.checkArgument(this.job instanceof TaskCallable,
                        "Job must be instance of TaskCallable");

        this.graph.taskScheduler().checkRequirement("schedule");

        @SuppressWarnings("unchecked")
        TaskCallable<V> job = (TaskCallable<V>) this.job;

        VortexTask<V> task = new VortexTask<>(this.genTaskId(), null, job);
        task.type(this.job.type());
        task.name(this.name);
        if (this.input != null) {
            task.input(this.input);
        }
        if (this.dependencies != null && !this.dependencies.isEmpty()) {
            for (Id depend : this.dependencies) {
                task.depends(depend);
            }
        }

        TaskScheduler scheduler = this.graph.taskScheduler();
        scheduler.schedule(task);

        return task;
    }

    private Id genTaskId() {
        return this.graph.getNextId(VortexType.TASK);
    }
}
