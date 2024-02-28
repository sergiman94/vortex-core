package com.vortex.client.structure.traverser;

import com.vortex.client.api.API;
import com.vortex.client.api.traverser.TraversersAPI;
import com.vortex.client.structure.constant.Direction;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class RepeatEdgeStep extends EdgeStep {

    @JsonProperty("max_times")
    public int maxTimes = 1;

    private RepeatEdgeStep() {
        super();
        this.maxTimes = 1;
    }

    public static Builder repeatStepBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("RepeatEdgeStep{direction=%s,labels=%s," +
                             "properties=%s,degree=%s,skipDegree=%s," +
                             "maxTimes=%s}",
                             this.direction, this.labels, this.properties,
                             this.degree, this.skipDegree, this.maxTimes);
    }

    public static class Builder {

        protected RepeatEdgeStep step;

        private Builder() {
            this.step = new RepeatEdgeStep();
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

        public Builder maxTimes(int maxTimes) {
            this.step.maxTimes = maxTimes;
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

        public RepeatEdgeStep build() {
            TraversersAPI.checkDegree(this.step.degree);
            TraversersAPI.checkSkipDegree(this.step.skipDegree,
                                          this.step.degree, API.NO_LIMIT);
            TraversersAPI.checkPositive(this.step.maxTimes, "max times");
            return this.step;
        }
    }
}
