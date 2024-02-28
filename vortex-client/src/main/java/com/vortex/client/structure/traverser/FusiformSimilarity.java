package com.vortex.client.structure.traverser;

import com.vortex.client.structure.graph.Vertex;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;

public class FusiformSimilarity {

    @JsonProperty("similars")
    private Map<Object, Set<Similar>> similarsMap;
    @JsonProperty("vertices")
    private Set<Vertex> vertices;

    public Map<Object, Set<Similar>> similarsMap() {
        return this.similarsMap;
    }

    public Set<Vertex> vertices() {
        return this.vertices;
    }

    public int size() {
        return this.similarsMap.size();
    }

    public Map.Entry<Object, Set<Similar>> first() {
        return this.similarsMap.entrySet().iterator().next();
    }

    public static class Similar {

        @JsonProperty
        private Object id;
        @JsonProperty
        private double score;
        @JsonProperty
        private Set<Object> intermediaries;

        public Object id() {
            return this.id;
        }

        public double score() {
            return this.score;
        }

        public Set<Object> intermediaries() {
            return this.intermediaries;
        }
    }
}
