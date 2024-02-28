package com.vortex.client.structure.version;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class Versions {

    @JsonProperty
    private Map<String, String> versions;

    public String get(String name) {
        return this.versions.get(name);
    }
}
