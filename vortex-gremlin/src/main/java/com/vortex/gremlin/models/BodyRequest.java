package com.vortex.gremlin.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BodyRequest {

    public BodyRequest() {}

    @JsonProperty("queryCode")
    String queryCode;

    public String getQueryCode() {
        return queryCode;
    }

    public void setQueryCode(String queryCode) {
        this.queryCode = queryCode;
    }
}
