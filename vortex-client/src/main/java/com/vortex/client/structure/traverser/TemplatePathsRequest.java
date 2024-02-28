package com.vortex.client.structure.traverser;

import com.vortex.client.api.traverser.TraversersAPI;
import com.vortex.client.structure.constant.Traverser;
import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class TemplatePathsRequest {

    @JsonProperty("sources")
    private VerticesArgs sources;
    @JsonProperty("targets")
    private VerticesArgs targets;
    @JsonProperty("steps")
    public List<RepeatEdgeStep> steps;
    @JsonProperty("with_ring")
    public boolean withRing = false;
    @JsonProperty("capacity")
    public long capacity = Traverser.DEFAULT_CAPACITY;
    @JsonProperty("limit")
    public long limit = Traverser.DEFAULT_PATHS_LIMIT;
    @JsonProperty("with_vertex")
    public boolean withVertex = false;

    private TemplatePathsRequest() {
        this.sources = null;
        this.targets = null;
        this.steps = new ArrayList<>();
        this.withRing = false;
        this.capacity = Traverser.DEFAULT_CAPACITY;
        this.limit = Traverser.DEFAULT_PATHS_LIMIT;
        this.withVertex = false;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("TemplatePathsRequest{sources=%s,targets=%s," +
                             "steps=%s,withRing=%s,capacity=%s,limit=%s," +
                             "withVertex=%s}", this.sources, this.targets,
                             this.steps, this.withRing, this.capacity,
                             this.limit, this.withVertex);
    }

    public static class Builder {

        private TemplatePathsRequest request;
        private VerticesArgs.Builder sourcesBuilder;
        private VerticesArgs.Builder targetsBuilder;
        private List<RepeatEdgeStep.Builder> stepBuilders;

        private Builder() {
            this.request = new TemplatePathsRequest();
            this.sourcesBuilder = VerticesArgs.builder();
            this.targetsBuilder = VerticesArgs.builder();
            this.stepBuilders = new ArrayList<>();
        }

        public VerticesArgs.Builder sources() {
            return this.sourcesBuilder;
        }

        public VerticesArgs.Builder targets() {
            return this.targetsBuilder;
        }

        public RepeatEdgeStep.Builder steps() {
            RepeatEdgeStep.Builder builder = RepeatEdgeStep.repeatStepBuilder();
            this.stepBuilders.add(builder);
            return builder;
        }

        public Builder withRing(boolean withRing) {
            this.request.withRing = withRing;
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

        public TemplatePathsRequest build() {
            this.request.sources = this.sourcesBuilder.build();
            E.checkArgument(this.request.sources != null,
                            "Source vertices can't be null");
            this.request.targets = this.targetsBuilder.build();
            E.checkArgument(this.request.targets != null,
                            "Target vertices can't be null");
            for (RepeatEdgeStep.Builder builder : this.stepBuilders) {
                this.request.steps.add(builder.build());
            }
            E.checkArgument(this.request.steps != null &&
                            !this.request.steps.isEmpty(),
                            "The steps can't be null or empty");
            TraversersAPI.checkCapacity(this.request.capacity);
            TraversersAPI.checkLimit(this.request.limit);
            return this.request;
        }
    }
}
