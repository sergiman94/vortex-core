package com.vortex.client.structure.auth;

import com.vortex.client.structure.constant.VortexType;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Group extends AuthElement {

    @JsonProperty("group_name")
    private String name;
    @JsonProperty("group_description")
    private String description;

    @JsonProperty("group_create")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date create;
    @JsonProperty("group_update")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date update;
    @JsonProperty("group_creator")
    protected String creator;

    @Override
    public String type() {
        return VortexType.GROUP.string();
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

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }
}
