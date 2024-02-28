package com.vortex.vortexdb.type.define;

public enum  Action implements SerialEnum {

    INSERT(1, "insert"),

    APPEND(2, "append"),

    ELIMINATE(3, "eliminate"),

    DELETE(4, "delete");

    private final byte code;
    private final String name;

    static {
        SerialEnum.register(Action.class);
    }

    Action(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string(){return this.name;}

    public static Action fromCode(byte code) {
        switch (code) {
            case 1:
                return INSERT;
            case 2:
                return APPEND;
            case 3:
                return ELIMINATE;
            case 4:
                return DELETE;
            default:
                throw new AssertionError("Unsupported action code: " + code);

        }
    }
}
