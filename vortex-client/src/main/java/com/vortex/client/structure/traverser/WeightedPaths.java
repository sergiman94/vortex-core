package com.vortex.client.structure.traverser;

import com.vortex.client.structure.graph.Vertex;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;

public class WeightedPaths {

    @JsonProperty
    private Map<Object, WeightedPath.Path> paths;
    @JsonProperty
    private Set<Vertex> vertices;

    public Map<Object, WeightedPath.Path> paths() {
        return this.paths;
    }

    public Set<Vertex> vertices() {
        return this.vertices;
    }
}
