package com.vortex.client.structure.traverser;

import com.vortex.client.structure.graph.Path;
import com.vortex.client.structure.graph.Vertex;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

public class Kout {

    @JsonProperty
    private int size;
    @JsonProperty("kout")
    private Set<Object> ids;
    @JsonProperty
    private List<Path> paths;
    @JsonProperty
    private Set<Vertex> vertices;

    public int size() {
        return this.size;
    }

    public Set<Object> ids() {
        return this.ids;
    }

    public List<Path> paths() {
        return this.paths;
    }

    public Set<Vertex> vertices() {
        return this.vertices;
    }
}
