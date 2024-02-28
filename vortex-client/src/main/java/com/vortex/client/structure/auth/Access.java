package com.vortex.client.structure.auth;

import com.vortex.client.structure.constant.VortexType;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Access extends AuthElement {

    @JsonProperty("group")
    private Object group;
    @JsonProperty("target")
    private Object target;
    @JsonProperty("access_permission")
    private VortexPermission permission;
    @JsonProperty("access_description")
    private String description;

    @JsonProperty("access_create")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date create;
    @JsonProperty("access_update")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date update;
    @JsonProperty("access_creator")
    protected String creator;

    @Override
    public String type() {
        return VortexType.ACCESS.string();
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

    public Object group() {
        return this.group;
    }

    public void group(Object group) {
        if (group instanceof Group) {
            group = ((Group) group).id();
        }
        this.group = group;
    }

    public Object target() {
        return this.target;
    }

    public void target(Object target) {
        if (target instanceof Target) {
            target = ((Target) target).id();
        }
        this.target = target;
    }

    public VortexPermission permission() {
        return this.permission;
    }

    public void permission(VortexPermission permission) {
        this.permission = permission;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }
}
