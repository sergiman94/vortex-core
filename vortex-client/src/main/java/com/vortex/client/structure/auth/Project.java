package com.vortex.client.structure.auth;

import com.vortex.client.structure.constant.VortexType;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class Project extends AuthElement {

    @JsonProperty("project_name")
    private String name;
    @JsonProperty("project_admin_group")
    private String adminGroup;
    @JsonProperty("project_op_group")
    private String opGroup;
    @JsonProperty("project_graphs")
    private Set<String> graphs;
    @JsonProperty("project_target")
    private String target;
    @JsonProperty("project_description")
    private String description;

    @JsonProperty("project_create")
    @JsonFormat(pattern = DATE_FORMAT)
    private Date create;
    @JsonProperty("project_update")
    @JsonFormat(pattern = DATE_FORMAT)
    private Date update;
    @JsonProperty("project_creator")
    private String creator;

    public Project() {
    }

    public Project(Object id) {
        this(id, null, null);
    }

    public Project(String name) {
        this(name, null);
    }

    public Project(String name, String description) {
        this(null, name, description);
    }

    public Project(Object id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }

    public String adminGroup() {
        return this.adminGroup;
    }

    public String opGroup() {
        return this.opGroup;
    }

    public Set<String> graphs() {
        return this.graphs;
    }

    public void graphs(Set<String> graphs) {
        if (graphs != null) {
            this.graphs = new HashSet<>(graphs);
        } else {
            this.graphs = null;
        }
    }

    public String target() {
        return this.target;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String type() {
        return VortexType.PROJECT.string();
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
}
