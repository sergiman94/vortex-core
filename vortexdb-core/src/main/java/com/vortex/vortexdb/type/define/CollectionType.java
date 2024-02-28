
package com.vortex.vortexdb.type.define;

public enum CollectionType implements SerialEnum {

    // Java Collection Framework
    JCF(1, "jcf"),

    // Eclipse Collection
    EC(2, "ec"),

    // FastUtil
    FU(3, "fu");

    private final byte code;
    private final String name;

    static {
        SerialEnum.register(CollectionType.class);
    }

    CollectionType(int code, String name) {
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

    public static CollectionType fromCode(byte code) {
        switch (code) {
            case 1:
                return JCF;
            case 2:
                return EC;
            case 3:
                return FU;
            default:
                throw new AssertionError(
                          "Unsupported collection code: " + code);
        }
    }
}
