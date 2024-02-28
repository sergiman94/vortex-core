package com.vortex.client.structure.constant;

public enum AggregateType {

    NONE(0, "none"),
    MAX(1, "max"),
    MIN(2, "min"),
    SUM(3, "sum"),
    OLD(4, "old");

    private final byte code;
    private final String name;

    AggregateType(int code, String name) {
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

    public boolean isNone() {
        return this == NONE;
    }

    public boolean isMax() {
        return this == MAX;
    }

    public boolean isMin() {
        return this == MIN;
    }

    public boolean isSum() {
        return this == SUM;
    }

    public boolean isNumber() {
        return this.isMax() || this.isMin() || this.isSum();
    }

    public boolean isOld() {
        return this == OLD;
    }

    public boolean isIndexable() {
        return this == NONE || this == MAX || this == MIN || this == OLD;
    }
}
