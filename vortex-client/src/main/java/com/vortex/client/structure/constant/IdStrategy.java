package com.vortex.client.structure.constant;

public enum IdStrategy {

    DEFAULT(0, "default"),

    AUTOMATIC(1, "automatic"),

    PRIMARY_KEY(2, "primary_key"),

    CUSTOMIZE_STRING(3, "customize_string"),

    CUSTOMIZE_NUMBER(4, "customize_number"),

    CUSTOMIZE_UUID(5, "customize_uuid");

    private byte code = 0;
    private String name = null;

    IdStrategy(int code, String name) {
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

    public boolean isAutomatic() {
        return this == IdStrategy.AUTOMATIC;
    }

    public boolean isCustomize() {
        return this == IdStrategy.CUSTOMIZE_STRING ||
               this == IdStrategy.CUSTOMIZE_NUMBER ||
               this == IdStrategy.CUSTOMIZE_UUID;
    }

    public boolean isCustomizeString() {
        return this == IdStrategy.CUSTOMIZE_STRING;
    }

    public boolean isCustomizeNumber() {
        return this == IdStrategy.CUSTOMIZE_NUMBER;
    }

    public boolean isCustomizeUuid() {
        return this == IdStrategy.CUSTOMIZE_UUID;
    }

    public boolean isPrimaryKey() {
        return this == IdStrategy.PRIMARY_KEY;
    }
}
