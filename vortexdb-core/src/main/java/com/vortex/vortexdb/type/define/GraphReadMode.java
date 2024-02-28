package com.vortex.vortexdb.type.define;

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

    public byte getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public boolean showOlap(){return this == ALL || this == OLAP_ONLY;}
}
