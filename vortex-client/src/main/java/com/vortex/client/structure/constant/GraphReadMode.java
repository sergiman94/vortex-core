package com.vortex.client.structure.constant;

public enum GraphReadMode {

    ALL(1, "all"),

    OLTP_ONLY(2, "oltp_only"),

    OLAP_ONLY(3, "olap_only");

    private final byte code;
    private final String name;

    private GraphReadMode(int code, String name) {
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

    public boolean showOlap() {
        return this == ALL || this == OLAP_ONLY;
    }
}
