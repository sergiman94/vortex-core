package com.vortex.vortexdb.type.define;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/*
* TODO: research about how cardinality works
* The cardinality of the values associated with given key for a particular element.
* */

public enum Cardinality implements SerialEnum {
    /**
     * Only a single value may be associated with the given key.
     */
    SINGLE(1, "single"),

    /**
     * Multiple values and duplicate values may be associated with the given
     * key.
     */
    LIST(2, "list"),

    /**
     * Multiple but distinct values may be associated with the given key.
     */
    SET(3, "set");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(Cardinality.class);
    }

    Cardinality(int code, String name) {
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

    public boolean single() {
        return this == SINGLE;
    }

    public boolean multiple() {
        return this == LIST || this == SET;
    }

    public static Cardinality convert(VertexProperty.Cardinality cardinality) {
        switch (cardinality) {
            case single:
                return SINGLE;
            case list:
                return LIST;
            case set:
                return SET;
            default:
                throw new AssertionError(String.format(
                        "Unrecognized cardinality: '%s'", cardinality));
        }
    }
}
