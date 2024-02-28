package com.vortex.client.structure.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LoginResult {

    @JsonProperty("token")
    private String token;

    public void token(String token) {
        this.token = token;
    }

    public String token() {
        return this.token;
    }
}
