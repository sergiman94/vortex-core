
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.type.define.SerialEnum;

public enum VortexPermission implements SerialEnum {

    NONE(0x00, "none"),

    READ(0x01, "read"),
    WRITE(0x02, "write"),
    DELETE(0x04, "delete"),
    EXECUTE(0x08, "execute"),

    ANY(0x7f, "any");

    private byte code;
    private String name;

    static {
        SerialEnum.register(VortexPermission.class);
    }

    VortexPermission(int code, String name) {
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

    public boolean match(VortexPermission other) {
        if (other == ANY) {
            return this == ANY;
        }
        return (this.code & other.code) != 0;
    }

    public static VortexPermission fromCode(byte code) {
        return SerialEnum.fromCode(VortexPermission.class, code);
    }
}
