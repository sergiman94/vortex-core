package com.vortex.client.structure.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Edges extends Pageable<Edge> {

    @JsonProperty
    private List<Edge> edges;

    @JsonCreator
    public Edges(@JsonProperty("edges") List<Edge> edges,
                 @JsonProperty("page") String page) {
        super(page);
        this.edges = edges;
    }

    @Override
    public List<Edge> results() {
        return this.edges;
    }
}
