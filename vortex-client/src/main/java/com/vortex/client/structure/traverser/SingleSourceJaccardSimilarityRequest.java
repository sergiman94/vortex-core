package com.vortex.client.structure.traverser;

import com.vortex.client.api.traverser.TraversersAPI;
import com.vortex.client.structure.constant.Traverser;
import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SingleSourceJaccardSimilarityRequest {

    @JsonProperty("vertex")
    private Object vertex;
    @JsonProperty("step")
    public EdgeStep step;
    @JsonProperty("top")
    public int top = 10;
    @JsonProperty("capacity")
    public long capacity = Traverser.DEFAULT_CAPACITY;

    private SingleSourceJaccardSimilarityRequest() {
        this.vertex = null;
        this.step = null;
        this.top = 10;
        this.capacity = Traverser.DEFAULT_CAPACITY;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("SingleSourceJaccardSimilarityRequest{vertex=%s," +
                             "step=%s,top=%s,capacity=%s}",
                             this.vertex, this.step, this.top, this.capacity);
    }

    public static class Builder {

        private SingleSourceJaccardSimilarityRequest request;
        private EdgeStep.Builder stepBuilder;

        private Builder() {
            this.request = new SingleSourceJaccardSimilarityRequest();
            this.stepBuilder = EdgeStep.builder();
        }

        public Builder vertex(Object vertex) {
            E.checkNotNull(vertex, "vertex");
            this.request.vertex = vertex;
            return this;
        }

        public EdgeStep.Builder step() {
            EdgeStep.Builder builder = EdgeStep.builder();
            this.stepBuilder = builder;
            return builder;
        }

        public Builder top(int top) {
            TraversersAPI.checkPositive(top, "top");
            this.request.top = top;
            return this;
        }

        public Builder capacity(long capacity) {
            TraversersAPI.checkCapacity(capacity);
            this.request.capacity = capacity;
            return this;
        }

        public SingleSourceJaccardSimilarityRequest build() {
            E.checkArgument(this.request.vertex != null,
                            "The vertex can't be null");
            this.request.step = this.stepBuilder.build();
            E.checkNotNull(this.request.step, "step");
            TraversersAPI.checkCapacity(this.request.capacity);
            E.checkArgument(this.request.top >= 0,
                            "The top must be >= 0, but got: %s",
                            this.request.top);
            return this.request;
        }
    }
}
