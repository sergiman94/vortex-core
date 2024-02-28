
package com.vortex.vortexdb.type.define;

public enum Frequency implements SerialEnum {

    DEFAULT(0, "default"),

    SINGLE(1, "single"),

    MULTIPLE(2, "multiple");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(Frequency.class);
    }

    Frequency(int code, String name) {
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
}
