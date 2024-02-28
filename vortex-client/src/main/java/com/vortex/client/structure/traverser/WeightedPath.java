package com.vortex.client.structure.traverser;

import com.vortex.client.structure.graph.Vertex;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

public class WeightedPath {

    @JsonProperty
    private Path path;
    @JsonProperty
    private Set<Vertex> vertices;

    public Path path() {
        return this.path;
    }

    public Set<Vertex> vertices() {
        return this.vertices;
    }

    public static class Path {

        @JsonProperty
        private double weight;
        @JsonProperty
        private List<Object> vertices;

        public double weight() {
            return this.weight;
        }

        public List<Object> vertices() {
            return this.vertices;
        }
    }
}
