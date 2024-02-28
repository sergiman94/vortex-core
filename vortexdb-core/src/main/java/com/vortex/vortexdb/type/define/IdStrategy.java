
package com.vortex.vortexdb.type.define;

public enum IdStrategy implements SerialEnum {

    DEFAULT(0, "default"),

    AUTOMATIC(1, "automatic"),

    PRIMARY_KEY(2, "primary_key"),

    CUSTOMIZE_STRING(3, "customize_string"),

    CUSTOMIZE_NUMBER(4, "customize_number"),

    CUSTOMIZE_UUID(5, "customize_uuid");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(IdStrategy.class);
    }

    IdStrategy(int code, String name) {
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

    public boolean isAutomatic() {
        return this == AUTOMATIC;
    }

    public boolean isPrimaryKey() {
        return this == PRIMARY_KEY;
    }

    public boolean isCustomized() {
        return this == CUSTOMIZE_STRING ||
               this == CUSTOMIZE_NUMBER ||
               this == CUSTOMIZE_UUID;
    }
}
