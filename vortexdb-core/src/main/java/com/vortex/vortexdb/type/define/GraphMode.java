package com.vortex.vortexdb.type.define;

public enum GraphMode {

    /*
     * None mode is regular mode
     * 1. Not allowed create schema with specified id
     * 2. Not support create vertex with id for AUTOMATIC id strategy
     */
    NONE(1, "none"),

    /*
     * Restoring mode is used to restore schema and graph data to an new graph.
     * 1. Support create schema with specified id
     * 2. Support create vertex with id for AUTOMATIC id strategy
     */
    RESTORING(2, "restoring"),

    /*
     * MERGING mode is used to merge schema and graph data to an existing graph.
     * 1. Not allowed create schema with specified id
     * 2. Support create vertex with id for AUTOMATIC id strategy
     */
    MERGING(3, "merging"),

    /*
     * LOADING mode used to load data via vortex-loader.
     */
    LOADING(4, "loading");

    private final byte code;
    private final String name;

    private GraphMode(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte getCode() {return code;}

    public String getName() {return name;}

    public boolean maintaining() { return this == RESTORING || this == MERGING; }

    public boolean loading() { return this == LOADING;}
}
