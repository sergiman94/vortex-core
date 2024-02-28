package com.vortex.client.structure.traverser;

import com.vortex.client.structure.graph.Path;
import com.vortex.client.structure.graph.Vertex;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

public class CustomizedCrosspoints {

    @JsonProperty
    private List<Object> crosspoints;
    @JsonProperty
    private List<Path> paths;
    @JsonProperty
    private Set<Vertex> vertices;

    public List<Object> crosspoints() {
        return this.crosspoints;
    }

    public List<Path> paths() {
        return this.paths;
    }

    public Set<Vertex> vertices() {
        return this.vertices;
    }
}
