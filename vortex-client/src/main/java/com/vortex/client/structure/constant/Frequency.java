package com.vortex.client.structure.constant;

public enum Frequency {

    DEFAULT(0, "default"),

    SINGLE(1, "single"),

    MULTIPLE(2, "multiple");

    private byte code = 0;
    private String name = null;

    Frequency(int code, String name) {
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
