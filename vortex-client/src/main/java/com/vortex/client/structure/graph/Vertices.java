package com.vortex.client.structure.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Vertices extends Pageable<Vertex> {

    @JsonProperty
    private List<Vertex> vertices;

    @JsonCreator
    public Vertices(@JsonProperty("vertices") List<Vertex> vertices,
                    @JsonProperty("page") String page) {
        super(page);
        this.vertices = vertices;
    }

    @Override
    public List<Vertex> results() {
        return this.vertices;
    }
}
