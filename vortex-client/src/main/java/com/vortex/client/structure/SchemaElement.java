package com.vortex.client.structure;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class SchemaElement extends Element {

    @JsonProperty("id")
    protected long id;
    @JsonProperty("name")
    protected String name;
    @JsonProperty("properties")
    protected Set<String> properties;
    @JsonProperty("check_exist")
    protected boolean checkExist;
    @JsonProperty("user_data")
    protected Map<String, Object> userdata;
    @JsonProperty("status")
    protected String status;

    public SchemaElement(String name) {
        this.name = name;
        this.properties = new ConcurrentSkipListSet<>();
        this.userdata = new ConcurrentHashMap<>();
        this.checkExist = true;
        this.status = null;
    }

    @Override
    public Long id() {
        return this.id;
    }

    public void resetId() {
        this.id = 0L;
    }

    public String name() {
        return this.name;
    }

    public Set<String> properties() {
        return this.properties;
    }

    public Map<String, Object> userdata() {
        return this.userdata;
    }

    public String status() {
        return this.status;
    }

    public boolean checkExist() {
        return this.checkExist;
    }

    public void checkExist(boolean checkExist) {
        this.checkExist = checkExist;
    }
}
