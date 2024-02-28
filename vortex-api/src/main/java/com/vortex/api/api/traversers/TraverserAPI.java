
package com.vortex.api.api.traversers;

import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.vortexdb.traversal.algorithm.steps.EdgeStep;
import com.vortex.vortexdb.type.define.Directions;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_MAX_DEGREE;

public class TraverserAPI extends API {

    protected static EdgeStep step(Vortex graph, Step step) {
        return new EdgeStep(graph, step.direction, step.labels, step.properties,
                            step.maxDegree, step.skipDegree);
    }

    protected static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                                 "maxDegree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.maxDegree, this.skipDegree);
        }
    }
}
