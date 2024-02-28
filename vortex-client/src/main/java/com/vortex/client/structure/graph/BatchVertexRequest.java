package com.vortex.client.structure.graph;

import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchVertexRequest {

    @JsonProperty("vertices")
    private List<Vertex> vertices;
    @JsonProperty("update_strategies")
    private Map<String, UpdateStrategy> updateStrategies;
    @JsonProperty("create_if_not_exist")
    private boolean createIfNotExist;

    public BatchVertexRequest() {
        this.vertices = null;
        this.updateStrategies = null;
        this.createIfNotExist = true;
    }

    public static Builder createBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("BatchVertexRequest{vertices=%s," +
                             "updateStrategies=%s,createIfNotExist=%s}",
                             this.vertices, this.updateStrategies,
                             this.createIfNotExist);
    }

    public static class Builder {

        private BatchVertexRequest req;

        public Builder() {
            this.req = new BatchVertexRequest();
        }

        public Builder vertices(List<Vertex> vertices) {
            this.req.vertices = vertices;
            return this;
        }

        public Builder updatingStrategies(Map<String, UpdateStrategy> map) {
            this.req.updateStrategies = new HashMap<>(map);
            return this;
        }

        public Builder updatingStrategy(String property,
                                        UpdateStrategy strategy) {
            this.req.updateStrategies.put(property, strategy);
            return this;
        }

        public Builder createIfNotExist(boolean createIfNotExist) {
            this.req.createIfNotExist = createIfNotExist;
            return this;
        }

        public BatchVertexRequest build() {
            E.checkArgumentNotNull(req, "BatchVertexRequest can't be null");
            E.checkArgumentNotNull(req.vertices,
                                   "Parameter 'vertices' can't be null");
            E.checkArgument(req.updateStrategies != null &&
                            !req.updateStrategies.isEmpty(),
                            "Parameter 'update_strategies' can't be empty");
            E.checkArgument(req.createIfNotExist == true,
                            "Parameter 'create_if_not_exist' " +
                            "dose not support false now");
            return this.req;
        }
    }
}
