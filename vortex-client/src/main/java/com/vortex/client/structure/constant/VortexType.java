package com.vortex.client.structure.constant;

public enum VortexType {

    // Schema
    VERTEX_LABEL(1, "vertexlabels"),
    EDGE_LABEL(2, "edgelabels"),
    PROPERTY_KEY(3, "propertykeys"),
    INDEX_LABEL(4, "indexlabels"),

    // Auth
    TARGET(50, "targets"),
    GROUP(51, "groups"),
    USER(52, "users"),
    ACCESS(53, "accesses"),
    BELONG(54, "belongs"),
    PROJECT(55, "projects"),
    LOGIN(56, "login"),
    LOGOUT(57, "logout"),
    TOKEN_VERIFY(58, "verify"),

    // Data
    VERTEX(101, "vertices"),
    EDGE(120, "edges"),

    // Variables
    VARIABLES(130, "variables"),

    // Task
    TASK(140, "tasks"),

    // Job
    JOB(150, "jobs"),

    // Gremlin
    GREMLIN(201, "gremlin"),

    GRAPHS(220, "graphs"),

    // Version
    VERSION(230, "versions"),

    // Metrics
    METRICS(240, "metrics");

    private int code;
    private String name = null;

    VortexType(int code, String name) {
        assert code < 256;
        this.code = code;
        this.name = name;
    }

    public int code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
