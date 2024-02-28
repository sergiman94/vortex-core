package com.vortex.client.structure.graph;

import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class BatchOlapPropertyRequest {

    @JsonProperty("vertices")
    private List<OlapVertex> vertices;
    @JsonProperty("property_key")
    private String propertyKey;

    public BatchOlapPropertyRequest() {
        this.vertices = null;
        this.propertyKey = null;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("BatchOlapPropertyRequest{vertices=%s," +
                             "propertyKey=%s}",
                             this.vertices, this.propertyKey);
    }

    public static class Builder {

        private BatchOlapPropertyRequest req;

        private Builder() {
            this.req = new BatchOlapPropertyRequest();
        }

        public Builder vertices(List<OlapVertex> vertices) {
            E.checkArgument(vertices != null && !vertices.isEmpty(),
                            "Parameter 'vertices' can't be null or empty");
            this.req.vertices = vertices;
            return this;
        }

        public Builder propertyKey(String propertyKey) {
            E.checkArgumentNotNull(propertyKey,
                                   "The property key can't be null");
            this.req.propertyKey = propertyKey;
            return this;
        }

        public BatchOlapPropertyRequest build() {
            E.checkArgumentNotNull(req,
                                   "BatchOlapPropertyRequest can't be null");
            E.checkArgumentNotNull(req.vertices,
                                   "Parameter 'vertices' can't be null");
            E.checkArgument(req.propertyKey != null,
                            "Parameter 'property_key' can't be empty");
            return this.req;
        }
    }

    public static class OlapVertex {

        @JsonProperty("id")
        public Object id;
        @JsonProperty("label")
        public String label;
        @JsonProperty("value")
        public Object value;

        @Override
        public String toString() {
            return String.format("OlapVertex{id=%s,label=%s,value=%s}",
                                 this.id, this.label, this.value);
        }

        public static class Builder {

            private OlapVertex vertex;

            public Builder() {
                this.vertex = new OlapVertex();
            }

            public Builder id(Object id) {
                E.checkArgumentNotNull(id, "The id of vertex can't be null");
                this.vertex.id = id;
                return this;
            }

            public Builder label(String label) {
                E.checkArgumentNotNull(label,
                                       "The label of vertex can't be null");
                this.vertex.label = label;
                return this;
            }

            public Builder value(Object value) {
                E.checkArgumentNotNull(value,
                                       "The value of vertex can't be null");
                this.vertex.value = value;
                return this;
            }

            public OlapVertex build() {
                E.checkArgumentNotNull(this.vertex.id,
                                       "The id of vertex can't be null");
                E.checkArgumentNotNull(this.vertex.label,
                                       "The label of vertex can't be null");
                E.checkArgumentNotNull(this.vertex.value,
                                       "The value of vertex can't be null");
                return this.vertex;
            }
        }
    }
}
