package com.vortex.client.structure.traverser;

import com.vortex.client.api.API;
import com.vortex.client.api.traverser.TraversersAPI;
import com.vortex.client.structure.constant.Direction;
import com.vortex.client.structure.constant.Traverser;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EdgeStep {

    @JsonProperty("direction")
    protected Direction direction;
    @JsonProperty("labels")
    protected List<String> labels;
    @JsonProperty("properties")
    protected Map<String, Object> properties;
    @JsonProperty("degree")
    protected long degree;
    @JsonProperty("skip_degree")
    protected long skipDegree;

    protected EdgeStep() {
        this.direction = Direction.BOTH;
        this.labels = new ArrayList<>();
        this.properties = new HashMap<>();
        this.degree = Traverser.DEFAULT_MAX_DEGREE;
        this.skipDegree = Traverser.DEFAULT_SKIP_DEGREE;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("EdgeStep{direction=%s,labels=%s,properties=%s," +
                             "degree=%s,skipDegree=%s}",
                             this.direction, this.labels, this.properties,
                             this.degree, this.skipDegree);
    }

    public static class Builder {

        protected EdgeStep step;

        private Builder() {
            this.step = new EdgeStep();
        }

        public Builder direction(Direction direction) {
            this.step.direction = direction;
            return this;
        }

        public Builder labels(List<String> labels) {
            this.step.labels = labels;
            return this;
        }

        public Builder labels(String label) {
            this.step.labels.add(label);
            return this;
        }

        public Builder properties(Map<String, Object> properties) {
            this.step.properties = properties;
            return this;
        }

        public Builder properties(String key, Object value) {
            this.step.properties.put(key, value);
            return this;
        }

        public Builder degree(long degree) {
            TraversersAPI.checkDegree(degree);
            this.step.degree = degree;
            return this;
        }

        public Builder skipDegree(long skipDegree) {
            TraversersAPI.checkSkipDegree(skipDegree, this.step.degree,
                                          API.NO_LIMIT);
            this.step.skipDegree = skipDegree;
            return this;
        }

        public EdgeStep build() {
            TraversersAPI.checkDegree(this.step.degree);
            TraversersAPI.checkSkipDegree(this.step.skipDegree,
                                          this.step.degree, API.NO_LIMIT);
            return this.step;
        }
    }
}
