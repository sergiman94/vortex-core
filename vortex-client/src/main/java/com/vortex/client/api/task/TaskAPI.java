package com.vortex.client.api.task;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.common.rest.ClientException;
import com.vortex.common.rest.RestResult;
import com.vortex.client.structure.Task;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.common.util.E;
import com.vortex.client.util.TaskCache;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TaskAPI extends API {

    private static final String PATH = "graphs/%s/tasks";
    private String graph;
    public static final String TASKS = "tasks";
    public static final String TASK_ID = "task_id";
    public static final long TASK_TIMEOUT = 60L;
    private static final long QUERY_INTERVAL = 500L;

    public TaskAPI(RestClient client, String graph) {
        super(client);
        this.path(String.format(PATH, graph));
        this.graph = graph;
    }

    @Override
    protected String type() {
        return VortexType.TASK.string();
    }

    public String graph() {
        return this.graph;
    }

    public List<Task> list(String status, long limit) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("limit", limit);
        if (status != null) {
            params.put("status", status);
        }
        RestResult result = this.client.get(this.path(), params);
        return result.readList(TASKS, Task.class);
    }

    public TasksWithPage list(String status, String page, long limit) {
        E.checkArgument(page != null, "The page can not be null");
        this.client.checkApiVersion("0.48", "getting tasks by paging");
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("limit", limit);
        params.put("page", page);
        if (status != null) {
            params.put("status", status);
        }
        RestResult result = this.client.get(this.path(), params);
        return result.readObject(TasksWithPage.class);
    }

    public List<Task> list(List<Long> ids) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("ids", ids);
        RestResult result = this.client.get(this.path(), params);
        return result.readList(TASKS, Task.class);
    }

    public Task get(long id) {
        RestResult result = this.client.get(this.path(), String.valueOf(id));
        return result.readObject(Task.class);
    }

    public void delete(long id) {
        this.client.delete(path(), String.valueOf(id));
    }

    public Task cancel(long id) {
        Map<String, Object> params = ImmutableMap.of("action", "cancel");
        RestResult result = this.client.put(path(), String.valueOf(id),
                                            ImmutableMap.of(), params);
        return result.readObject(Task.class);
    }

    public Task waitUntilTaskSuccess(long taskId, long seconds) {
        if (taskId == 0) {
            return null;
        }
        long passes = seconds * 1000 / QUERY_INTERVAL;
        try {
            for (long pass = 0; ; pass++) {
                Task task = this.getFromCache(taskId);
                if (task.success()) {
                    return task;
                } else if (task.completed()) {
                    throw new ClientException(
                              "Task '%s' is %s, result is '%s'",
                              taskId, task.status(), task.result());
                }
                if (pass >= passes) {
                    break;
                }
                try {
                    // Query every half second from cache to decrease waiting
                    // time because restful query is executed per second
                    Thread.sleep(QUERY_INTERVAL);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
            throw new ClientException(
                      "Task '%s' not completed in %s seconds, " +
                      "it can still be queried by task-get API",
                      taskId, seconds);
        } finally {
            // Stop querying this task info whatever
            this.removeFromCache(taskId);
        }
    }

    private Task getFromCache(long taskId) {
        return TaskCache.instance().get(this, taskId);
    }

    private void removeFromCache(long taskId) {
        TaskCache.instance().remove(this, taskId);
    }

    public static long parseTaskId(Map<String, Object> task) {
        E.checkState(task.size() == 1 && task.containsKey(TASK_ID),
                     "Task must be formatted to {\"%s\" : id}, but got %s",
                     TASK_ID, task);
        Object taskId = task.get(TASK_ID);
        E.checkState(taskId instanceof Number,
                     "Task id must be number, but got '%s'", taskId);
        return ((Number) taskId).longValue();
    }
}
