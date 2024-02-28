package com.vortex.client.structure.traverser;

import com.vortex.client.structure.graph.Vertex;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

public class PathsWithVertices {

    @JsonProperty
    private List<Paths> paths;
    @JsonProperty
    private Set<Vertex> vertices;

    public List<Paths> paths() {
        return this.paths;
    }

    public Set<Vertex> vertices() {
        return this.vertices;
    }

    public static class Paths {

        @JsonProperty
        private List<Object> objects;
        @JsonProperty
        private List<Double> weights;

        public List<Object> objects() {
            return this.objects;
        }

        public List<Double> weights() {
            return this.weights;
        }
    }
}
