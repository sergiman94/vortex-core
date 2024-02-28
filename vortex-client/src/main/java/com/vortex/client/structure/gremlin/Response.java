package com.vortex.client.structure.gremlin;

import com.vortex.client.driver.GraphManager;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class Response {

    public static class Status {
        @JsonProperty
        private String message;
        @JsonProperty
        private int code;
        @JsonProperty
        private Map<String, ?> attributes;

        public String message() {
            return message;
        }

        public int code() {
            return code;
        }

        public Map<String, ?> attributes() {
            return attributes;
        }
    }

    @JsonProperty
    private String requestId;
    @JsonProperty
    private Status status;
    @JsonProperty
    private ResultSet result;

    public void graphManager(GraphManager graphManager) {
        this.result.graphManager(graphManager);
    }

    public String requestId() {
        return this.requestId;
    }

    public Status status() {
        return this.status;
    }

    public ResultSet result() {
        return this.result;
    }
}
