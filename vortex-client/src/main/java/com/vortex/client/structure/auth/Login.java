package com.vortex.client.structure.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Login {

    @JsonProperty("user_name")
    private String name;

    @JsonProperty("user_password")
    private String password;

    public void name(String name) {
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    public void password(String password) {
        this.password = password;
    }

    public String password() {
        return this.password;
    }
}
