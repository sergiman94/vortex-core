package com.vortex.client.structure.auth;

import com.vortex.client.structure.constant.VortexType;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Belong extends AuthElement {

    @JsonProperty("user")
    private Object user;
    @JsonProperty("group")
    private Object group;
    @JsonProperty("belong_description")
    private String description;

    @JsonProperty("belong_create")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date create;
    @JsonProperty("belong_update")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date update;
    @JsonProperty("belong_creator")
    protected String creator;

    @Override
    public String type() {
        return VortexType.BELONG.string();
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

    public Object user() {
        return this.user;
    }

    public void user(Object user) {
        if (user instanceof User) {
            user = ((User) user).id();
        }
        this.user = user;
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

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }
}
