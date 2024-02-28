package com.vortex.client.structure.constant;

public enum IndexType {

    // For secondary query
    SECONDARY(1, "secondary"),

    // For range query
    RANGE(2, "range"),

    // For full-text query (not supported now)
    SEARCH(3, "search"),

    // For prefix + range query
    SHARD(4, "shard"),

    // For unique properties
    UNIQUE(5, "unique");

    private byte code = 0;
    private String name = null;

    IndexType(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
