package com.vortex.client.structure.graph;

import com.vortex.client.structure.GraphElement;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public abstract class Pageable<T extends GraphElement> {

    @JsonProperty
    private String page;

    @JsonCreator
    public Pageable(@JsonProperty("page") String page) {
        this.page = page;
    }

    public abstract List<T> results();

    public String page() {
        return this.page;
    }
}
