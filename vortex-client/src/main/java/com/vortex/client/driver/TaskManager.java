package com.vortex.client.driver;

import com.vortex.client.api.task.TaskAPI;
import com.vortex.client.api.task.TasksWithPage;
import com.vortex.client.client.RestClient;
import com.vortex.client.structure.Task;

import java.util.List;

public class TaskManager {

    private TaskAPI taskAPI;

    public TaskManager(RestClient client, String graph) {
        this.taskAPI = new TaskAPI(client, graph);
    }

    public List<Task> list() {
        return this.list(-1);
    }

    public List<Task> list(long limit) {
        return this.taskAPI.list(null, limit);
    }

    public List<Task> list(List<Long> ids) {
        return this.taskAPI.list(ids);
    }

    public List<Task> list(String status) {
        return this.list(status, -1L);
    }

    public List<Task> list(String status, long limit) {
        return this.taskAPI.list(status, limit);
    }

    public TasksWithPage list(String status, String page, long limit) {
        return this.taskAPI.list(status, page, limit);
    }

    public Task get(long id) {
        return this.taskAPI.get(id);
    }

    public void delete(long id) {
        this.taskAPI.delete(id);
    }

    public Task cancel(long id) {
        return this.taskAPI.cancel(id);
    }

    public Task waitUntilTaskCompleted(long taskId, long seconds) {
        return this.taskAPI.waitUntilTaskSuccess(taskId, seconds);
    }
}
