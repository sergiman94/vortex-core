package com.vortex.client.structure.auth;

import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class User extends AuthElement {

    @JsonProperty("user_name")
    private String name;
    @JsonProperty("user_password")
    private String password;
    @JsonProperty("user_phone")
    private String phone;
    @JsonProperty("user_email")
    private String email;
    @JsonProperty("user_avatar")
    private String avatar;
    @JsonProperty("user_description")
    private String description;

    @JsonProperty("user_create")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date create;
    @JsonProperty("user_update")
    @JsonFormat(pattern = DATE_FORMAT)
    protected Date update;
    @JsonProperty("user_creator")
    protected String creator;

    @Override
    public String type() {
        return VortexType.USER.string();
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

    public String password() {
        return this.password;
    }

    public void password(String password) {
        this.password = password;
    }

    public String phone() {
        return this.phone;
    }

    public void phone(String phone) {
        this.phone = phone;
    }

    public String email() {
        return this.email;
    }

    public void email(String email) {
        this.email = email;
    }

    public String avatar() {
        return this.avatar;
    }

    public void avatar(String avatar) {
        this.avatar = avatar;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    public static class UserRole {

        @JsonProperty("roles")
        private Map<String, Map<VortexPermission, List<VortexResource>>> roles;

        public Map<String, Map<VortexPermission, List<VortexResource>>> roles() {
            return Collections.unmodifiableMap(this.roles);
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }
    }
}
