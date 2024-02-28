package com.vortex.client.structure.auth;

import com.vortex.client.structure.constant.VortexType;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Target extends AuthElement {

    @JsonProperty("target_name")
    private String name;
    @JsonProperty("target_graph")
    private String graph;
    @JsonProperty("target_url")
    private String url;
    @JsonProperty("target_resources")
    private List<VortexResource> resources;

    @JsonProperty("target_create")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date create;
    @JsonProperty("target_update")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date update;
    @JsonProperty("target_creator")
    protected String creator;

    @Override
    public String type() {
        return VortexType.TARGET.string();
    }

    @Override
    public Date createTime() {
        return this.create;
    }

    @Override
    public Date updateTime() {
        return this.update;
    }

    @Override
    public String creator() {
        return this.creator;
    }

    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }

    public String graph() {
        return this.graph;
    }

    public void graph(String graph) {
        this.graph = graph;
    }

    public String url() {
        return this.url;
    }

    public void url(String url) {
        this.url = url;
    }

    public VortexResource resource() {
        if (this.resources == null || this.resources.size() != 1) {
            return null;
        }
        return this.resources.get(0);
    }

    public List<VortexResource> resources() {
        if (this.resources == null) {
            return null;
        }
        return Collections.unmodifiableList(this.resources);
    }

    public void resources(List<VortexResource> resources) {
        this.resources = resources;
    }

    public void resources(VortexResource... resources) {
        this.resources = Arrays.asList(resources);
    }
}
