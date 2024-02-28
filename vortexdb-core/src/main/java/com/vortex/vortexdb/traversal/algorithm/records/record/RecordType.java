
package com.vortex.vortexdb.traversal.algorithm.records.record;

import com.vortex.vortexdb.type.define.SerialEnum;

public enum RecordType implements SerialEnum {

    // One key with one int value
    INT(1, "int"),

    // One key with multi unique values
    SET(2, "set"),

    // One key with multi values
    ARRAY(3, "array");

    private final byte code;
    private final String name;

    static {
        SerialEnum.register(RecordType.class);
    }

    RecordType(int code, String name) {
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

    public static RecordType fromCode(byte code) {
        switch (code) {
            case 1:
                return INT;
            case 2:
                return SET;
            case 3:
                return ARRAY;
            default:
                throw new AssertionError("Unsupported record code: " + code);
        }
    }
}
