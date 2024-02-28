package com.vortex.client.api.task;

import com.vortex.client.structure.Task;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TasksWithPage {

    @JsonProperty
    private String page;
    @JsonProperty
    private List<Task> tasks;

    public String page() {
        return this.page;
    }

    public List<Task> tasks() {
        return this.tasks;
    }
}
