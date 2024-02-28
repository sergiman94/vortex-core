package com.vortex.vortexdb.type.define;

public enum AggregateType implements SerialEnum {
    NONE(0, "none"),
    MAX(1, "max"),
    MIN(2, "min"),
    SUM(3, "sum"),
    OLD(4, "old"),
    SET(5, "set"),
    LIST(6, "list");

    private final byte code;
    private final String name;

    static {
        SerialEnum.register(AggregateType.class);
    }

    AggregateType(int code, String name) {
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

    public boolean isSet() {
        return this == SET;
    }

    public boolean isList() {
        return this == LIST;
    }

    public boolean isUnion() {
        return this == SET || this == LIST;
    }

    public boolean isIndexable() {
        return this == NONE || this == MAX || this == MIN || this == OLD;
    }


}
