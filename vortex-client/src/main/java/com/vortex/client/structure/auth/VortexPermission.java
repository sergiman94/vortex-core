package com.vortex.client.structure.auth;

public enum VortexPermission {

    NONE(0x00),

    READ(0x01),
    WRITE(0x02),
    DELETE(0x04),
    EXECUTE(0x08),

    ANY(0x7f);

    private final byte code;

    VortexPermission(int code) {
        assert code < 256;
        this.code = (byte) code;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name().toLowerCase();
    }

    public boolean match(VortexPermission other) {
        if (other == ANY) {
            return this == ANY;
        }
        return (this.code & other.code) != 0;
    }
}
