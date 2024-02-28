
package com.vortex.vortexdb.type.define;

public enum WriteType implements SerialEnum {

    // OLTP property key
    OLTP(1, "oltp"),

    // OLAP property key without index
    OLAP_COMMON(2, "olap_common"),

    // OLAP property key with secondary index
    OLAP_SECONDARY(3, "olap_secondary"),

    // OLAP property key with range index
    OLAP_RANGE(4, "olap_range");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(WriteType.class);
    }

    WriteType(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public boolean oltp() {
        return this == OLTP;
    }

    public boolean olap() {
        return this == OLAP_COMMON ||
               this == OLAP_RANGE ||
               this == OLAP_SECONDARY;
    }
}
