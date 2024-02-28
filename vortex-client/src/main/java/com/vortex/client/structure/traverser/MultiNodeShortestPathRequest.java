package com.vortex.client.structure.traverser;

import com.vortex.client.api.traverser.TraversersAPI;
import com.vortex.client.structure.constant.Traverser;
import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MultiNodeShortestPathRequest {

    @JsonProperty("vertices")
    public VerticesArgs vertices;
    @JsonProperty("step")
    public EdgeStep step;
    @JsonProperty("max_depth")
    public int maxDepth;
    @JsonProperty("capacity")
    public long capacity = Traverser.DEFAULT_CAPACITY;
    @JsonProperty("with_vertex")
    public boolean withVertex = false;

    private MultiNodeShortestPathRequest() {
        this.vertices = null;
        this.step = null;
        this.maxDepth = Traverser.DEFAULT_MAX_DEPTH;
        this.capacity = Traverser.DEFAULT_CAPACITY;
        this.withVertex = false;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("MultiNodeShortestPathRequest{vertices=%s," +
                             "step=%s,maxDepth=%s,capacity=%s,withVertex=%s}",
                             this.vertices, this.step, this.maxDepth,
                             this.capacity, this.withVertex);
    }

    public static class Builder {

        private MultiNodeShortestPathRequest request;
        private VerticesArgs.Builder verticesBuilder;
        private EdgeStep.Builder stepBuilder;

        private Builder() {
            this.request = new MultiNodeShortestPathRequest();
            this.verticesBuilder = VerticesArgs.builder();
            this.stepBuilder = EdgeStep.builder();
        }

        public VerticesArgs.Builder vertices() {
            return this.verticesBuilder;
        }

        public EdgeStep.Builder step() {
            EdgeStep.Builder builder = EdgeStep.builder();
            this.stepBuilder = builder;
            return builder;
        }

        public Builder maxDepth(int maxDepth) {
            TraversersAPI.checkPositive(maxDepth, "max depth");
            this.request.maxDepth = maxDepth;
            return this;
        }

        public Builder capacity(long capacity) {
            TraversersAPI.checkCapacity(capacity);
            this.request.capacity = capacity;
            return this;
        }

        public Builder withVertex(boolean withVertex) {
            this.request.withVertex = withVertex;
            return this;
        }

        public MultiNodeShortestPathRequest build() {
            this.request.vertices = this.verticesBuilder.build();
            E.checkArgument(this.request.vertices != null,
                            "The vertices can't be null");
            this.request.step = this.stepBuilder.build();
            E.checkArgument(this.request.step != null,
                            "The step can't be null");
            TraversersAPI.checkPositive(this.request.maxDepth, "max depth");
            TraversersAPI.checkCapacity(this.request.capacity);
            return this.request;
        }
    }
}
