
package com.vortex.vortexdb.type.define;

public enum SchemaStatus implements SerialEnum {

    CREATED(1, "created"),

    CREATING(2, "creating"),

    REBUILDING(3, "rebuilding"),

    DELETING(4, "deleting"),

    UNDELETED(5, "undeleted"),

    INVALID(6, "invalid");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(SchemaStatus.class);
    }

    SchemaStatus(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public boolean ok() {
        return this == CREATED;
    }

    public boolean deleting() {
        return this == DELETING || this == UNDELETED;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}