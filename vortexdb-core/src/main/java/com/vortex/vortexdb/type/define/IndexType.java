
package com.vortex.vortexdb.type.define;

import com.vortex.vortexdb.type.VortexType;

public enum IndexType implements SerialEnum {

    // For secondary query
    SECONDARY(1, "secondary"),

    // For range query
    RANGE(2, "range"),
    RANGE_INT(21, "range_int"),
    RANGE_FLOAT(22, "range_float"),
    RANGE_LONG(23, "range_long"),
    RANGE_DOUBLE(24, "range_double"),

    // For full-text query (not supported now)
    SEARCH(3, "search"),

    // For prefix + range query
    SHARD(4, "shard"),

    // For unique index
    UNIQUE(5, "unique");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(IndexType.class);
    }

    IndexType(int code, String name) {
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

    public VortexType type() {
        switch (this) {
            case SECONDARY:
                return VortexType.SECONDARY_INDEX;
            case RANGE_INT:
                return VortexType.RANGE_INT_INDEX;
            case RANGE_FLOAT:
                return VortexType.RANGE_FLOAT_INDEX;
            case RANGE_LONG:
                return VortexType.RANGE_LONG_INDEX;
            case RANGE_DOUBLE:
                return VortexType.RANGE_DOUBLE_INDEX;
            case SEARCH:
                return VortexType.SEARCH_INDEX;
            case SHARD:
                return VortexType.SHARD_INDEX;
            case UNIQUE:
                return VortexType.UNIQUE_INDEX;
            default:
                throw new AssertionError(String.format(
                          "Unknown index type '%s'", this));
        }
    }

    public boolean isString() {
        return this == SECONDARY || this == SEARCH ||
               this == SHARD || this == UNIQUE;
    }

    public boolean isNumeric() {
        return this == RANGE_INT || this == RANGE_FLOAT ||
               this == RANGE_LONG || this == RANGE_DOUBLE ||
               this == SHARD;
    }

    public boolean isSecondary() {
        return this == SECONDARY;
    }

    public boolean isRange() {
        return this == RANGE_INT || this == RANGE_FLOAT ||
               this == RANGE_LONG || this == RANGE_DOUBLE;
    }

    public boolean isSearch() {
        return this == SEARCH;
    }

    public boolean isShard() {
        return this == SHARD;
    }

    public boolean isUnique() {
        return this == UNIQUE;
    }
}
