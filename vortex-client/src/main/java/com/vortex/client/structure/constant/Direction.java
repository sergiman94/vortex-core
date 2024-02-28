package com.vortex.client.structure.constant;

public enum Direction {

    OUT(1, "out"),

    IN(2, "in"),

    BOTH(3, "both");

    private byte code = 0;
    private String name = null;

    Direction(int code, String name) {
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
