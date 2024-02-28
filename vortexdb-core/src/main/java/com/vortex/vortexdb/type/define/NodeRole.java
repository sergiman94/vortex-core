

package com.vortex.vortexdb.type.define;

public enum NodeRole implements SerialEnum {

    MASTER(1, "master"),

    WORKER(2, "worker"),

    COMPUTER(3, "computer");

    private final byte code;
    private final String name;

    private NodeRole(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    static {
        SerialEnum.register(NodeRole.class);
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public boolean master() {
        return this == MASTER;
    }

    public boolean worker() {
        return this == WORKER;
    }

    public boolean computer() {
        return this == COMPUTER;
    }
}
