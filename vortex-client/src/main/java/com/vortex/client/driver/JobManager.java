package com.vortex.client.driver;

import com.vortex.client.api.job.RebuildAPI;
import com.vortex.client.api.task.TaskAPI;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.schema.EdgeLabel;
import com.vortex.client.structure.schema.IndexLabel;
import com.vortex.client.structure.schema.VertexLabel;

import static com.vortex.client.api.task.TaskAPI.TASK_TIMEOUT;

public class JobManager {

    private RebuildAPI rebuildAPI;
    private TaskAPI taskAPI;

    public JobManager(RestClient client, String graph) {
        this.rebuildAPI = new RebuildAPI(client, graph);
        this.taskAPI = new TaskAPI(client, graph);
    }

    public void rebuild(VertexLabel vertexLabel) {
        this.rebuild(vertexLabel, TASK_TIMEOUT);
    }

    public void rebuild(VertexLabel vertexLabel, long seconds) {
        long task = this.rebuildAPI.rebuild(vertexLabel);
        this.taskAPI.waitUntilTaskSuccess(task, seconds);
    }

    public long rebuildAsync(VertexLabel vertexLabel) {
        return this.rebuildAPI.rebuild(vertexLabel);
    }

    public void rebuild(EdgeLabel edgeLabel) {
        this.rebuild(edgeLabel, TASK_TIMEOUT);
    }

    public void rebuild(EdgeLabel edgeLabel, long seconds) {
        long task = this.rebuildAPI.rebuild(edgeLabel);
        this.taskAPI.waitUntilTaskSuccess(task, seconds);
    }

    public long rebuildAsync(EdgeLabel edgeLabel) {
        return this.rebuildAPI.rebuild(edgeLabel);
    }

    public void rebuild(IndexLabel indexLabel) {
        this.rebuild(indexLabel, TASK_TIMEOUT);
    }

    public void rebuild(IndexLabel indexLabel, long seconds) {
        long task = this.rebuildAPI.rebuild(indexLabel);
        this.taskAPI.waitUntilTaskSuccess(task, seconds);
    }

    public long rebuildAsync(IndexLabel indexLabel) {
        return this.rebuildAPI.rebuild(indexLabel);
    }
}
