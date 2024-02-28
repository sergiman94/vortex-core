package com.vortex.client.structure.graph;

import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchEdgeRequest {

    @JsonProperty("edges")
    private List<Edge> edges;
    @JsonProperty("update_strategies")
    private Map<String, UpdateStrategy> updateStrategies;
    @JsonProperty("check_vertex")
    private boolean checkVertex;
    @JsonProperty("create_if_not_exist")
    private boolean createIfNotExist;

    public BatchEdgeRequest() {
        this.edges = null;
        this.updateStrategies = null;
        this.checkVertex = false;
        this.createIfNotExist = true;
    }

    public static Builder createBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("BatchEdgeRequest{edges=%s," +
                             "updateStrategies=%s," +
                             "checkVertex=%s,createIfNotExist=%s}",
                             this.edges, this.updateStrategies,
                             this.checkVertex, this.createIfNotExist);
    }

    public static class Builder {

        private BatchEdgeRequest req;

        public Builder() {
            this.req = new BatchEdgeRequest();
        }

        public Builder edges(List<Edge> edges) {
            this.req.edges = edges;
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

        public Builder checkVertex(boolean checkVertex) {
            this.req.checkVertex = checkVertex;
            return this;
        }

        public Builder createIfNotExist(boolean createIfNotExist) {
            this.req.createIfNotExist = createIfNotExist;
            return this;
        }

        public BatchEdgeRequest build() {
            E.checkArgumentNotNull(req, "BatchEdgeRequest can't be null");
            E.checkArgumentNotNull(req.edges,
                                   "Parameter 'edges' can't be null");
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
