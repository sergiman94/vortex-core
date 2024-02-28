package com.vortex.client.structure.traverser;

import com.vortex.client.api.traverser.TraversersAPI;
import com.vortex.client.structure.constant.Traverser;
import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PathsRequest {

    @JsonProperty("sources")
    private VerticesArgs sources;
    @JsonProperty("targets")
    private VerticesArgs targets;
    @JsonProperty("step")
    public EdgeStep step;
    @JsonProperty("max_depth")
    public int depth;
    @JsonProperty("nearest")
    public boolean nearest = false;
    @JsonProperty("capacity")
    public long capacity = Traverser.DEFAULT_CAPACITY;
    @JsonProperty("limit")
    public long limit = Traverser.DEFAULT_LIMIT;
    @JsonProperty("with_vertex")
    public boolean withVertex = false;

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("PathRequest{sources=%s,targets=%s,step=%s," +
                             "maxDepth=%s,nearest=%s,capacity=%s," +
                             "limit=%s,withVertex=%s}",
                             this.sources, this.targets, this.step, this.depth,
                             this.nearest, this.capacity,
                             this.limit, this.withVertex);
    }

    public static class Builder {

        private PathsRequest request;
        private EdgeStep.Builder stepBuilder;
        private VerticesArgs.Builder sourcesBuilder;
        private VerticesArgs.Builder targetsBuilder;

        private Builder() {
            this.request = new PathsRequest();
            this.stepBuilder = EdgeStep.builder();
            this.sourcesBuilder = VerticesArgs.builder();
            this.targetsBuilder = VerticesArgs.builder();
        }

        public VerticesArgs.Builder sources() {
            return this.sourcesBuilder;
        }

        public VerticesArgs.Builder targets() {
            return this.targetsBuilder;
        }

        public EdgeStep.Builder step() {
            EdgeStep.Builder builder = EdgeStep.builder();
            this.stepBuilder = builder;
            return builder;
        }

        public Builder maxDepth(int maxDepth) {
            TraversersAPI.checkPositive(maxDepth, "max depth");
            this.request.depth = maxDepth;
            return this;
        }

        public Builder nearest(boolean nearest) {
            this.request.nearest = nearest;
            return this;
        }

        public Builder capacity(long capacity) {
            TraversersAPI.checkCapacity(capacity);
            this.request.capacity = capacity;
            return this;
        }

        public Builder limit(long limit) {
            TraversersAPI.checkLimit(limit);
            this.request.limit = limit;
            return this;
        }

        public Builder withVertex(boolean withVertex) {
            this.request.withVertex = withVertex;
            return this;
        }

        public PathsRequest build() {
            this.request.sources = this.sourcesBuilder.build();
            E.checkArgument(this.request.sources != null,
                            "Source vertices can't be null");
            this.request.targets = this.targetsBuilder.build();
            E.checkArgument(this.request.targets != null,
                            "Target vertices can't be null");
            this.request.step = this.stepBuilder.build();
            E.checkNotNull(this.request.step, "The steps can't be null");
            TraversersAPI.checkPositive(this.request.depth, "max depth");
            TraversersAPI.checkCapacity(this.request.capacity);
            TraversersAPI.checkLimit(this.request.limit);
            return this.request;
        }
    }
}
