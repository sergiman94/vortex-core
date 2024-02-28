package com.vortex.client.structure.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TokenPayload {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("user_name")
    private String username;

    private TokenPayload() {
    }

    public String userId() {
        return this.userId;
    }

    public void userId(String userId) {
        this.userId = userId;
    }

    public String username() {
        return this.username;
    }

    public void username(String username) {
        this.username = username;
    }
}
