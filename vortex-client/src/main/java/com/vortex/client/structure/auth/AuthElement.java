package com.vortex.client.structure.auth;

import com.vortex.client.structure.Element;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AuthElement extends Element {

    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    @JsonProperty("id")
    protected Object id;

    @Override
    public Object id() {
        return this.id;
    }

    public abstract Date createTime();

    public abstract Date updateTime();

    public abstract String creator();
}
