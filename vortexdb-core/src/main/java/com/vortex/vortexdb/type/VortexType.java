
package com.vortex.vortexdb.type;

import com.vortex.vortexdb.type.define.SerialEnum;

import java.util.HashMap;
import java.util.Map;

public enum VortexType implements SerialEnum {

    UNKNOWN(0, "UNKNOWN"),

    /* Schema types */
    VERTEX_LABEL(1, "VL"),
    EDGE_LABEL(2, "EL"),
    PROPERTY_KEY(3, "PK"),
    INDEX_LABEL(4, "IL"),

    COUNTER(50, "C"),

    /* Data types */
    VERTEX(101, "V"),
    // System meta
    SYS_PROPERTY(102, "S"),
    // Property
    PROPERTY(103, "U"),
    // Vertex aggregate property
    AGGR_PROPERTY_V(104, "VP"),
    // Edge aggregate property
    AGGR_PROPERTY_E(105, "EP"),
    // Olap property
    OLAP(106, "AP"),
    // Edge
    EDGE(120, "E"),
    // Edge's direction is OUT for the specified vertex
    EDGE_OUT(130, "O"),
    // Edge's direction is IN for the specified vertex
    EDGE_IN(140, "I"),

    SECONDARY_INDEX(150, "SI"),
    VERTEX_LABEL_INDEX(151, "VI"),
    EDGE_LABEL_INDEX(152, "EI"),
    RANGE_INT_INDEX(160, "II"),
    RANGE_FLOAT_INDEX(161, "FI"),
    RANGE_LONG_INDEX(162, "LI"),
    RANGE_DOUBLE_INDEX(163, "DI"),
    SEARCH_INDEX(170, "AI"),
    SHARD_INDEX(175, "HI"),
    UNIQUE_INDEX(178, "UI"),

    TASK(180, "T"),

    // System schema
    SYS_SCHEMA(250, "SS"),

    MAX_TYPE(255, "~");

    private byte type = 0;
    private String name;

    private static final Map<String, VortexType> ALL_NAME = new HashMap<>();

    static {
        SerialEnum.register(VortexType.class);
        for (VortexType type : values()) {
            ALL_NAME.put(type.name, type);
        }
    }

    VortexType(int type, String name) {
        assert type < 256;
        this.type = (byte) type;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.type;
    }

    public String string() {
        return this.name;
    }

    public String readableName() {
        return this.name().replace('_', ' ').toLowerCase();
    }

    public boolean isSchema() {
        return this == VortexType.VERTEX_LABEL ||
               this == VortexType.EDGE_LABEL ||
               this == VortexType.PROPERTY_KEY ||
               this == VortexType.INDEX_LABEL;
    }

    public boolean isGraph() {
        return this.isVertex() || this.isEdge() ;
    }

    public boolean isVertex() {
        return this == VortexType.VERTEX;
    }

    public boolean isEdge() {
        return this == EDGE || this == EDGE_OUT || this == EDGE_IN;
    }

    public boolean isIndex() {
        return this == VERTEX_LABEL_INDEX || this == EDGE_LABEL_INDEX ||
               this == SECONDARY_INDEX || this == SEARCH_INDEX ||
               this == RANGE_INT_INDEX || this == RANGE_FLOAT_INDEX ||
               this == RANGE_LONG_INDEX || this == RANGE_DOUBLE_INDEX ||
               this == SHARD_INDEX || this == UNIQUE_INDEX;
    }

    public boolean isStringIndex() {
        return this == VERTEX_LABEL_INDEX || this == EDGE_LABEL_INDEX ||
               this == SECONDARY_INDEX || this == SEARCH_INDEX ||
               this == SHARD_INDEX || this == UNIQUE_INDEX;
    }

    public boolean isNumericIndex() {
        return this == RANGE_INT_INDEX || this == RANGE_FLOAT_INDEX ||
               this == RANGE_LONG_INDEX || this == RANGE_DOUBLE_INDEX ||
               this == SHARD_INDEX;
    }

    public boolean isSecondaryIndex() {
        return this == VERTEX_LABEL_INDEX || this == EDGE_LABEL_INDEX ||
               this == SECONDARY_INDEX;
    }

    public boolean isSearchIndex() {
        return this == SEARCH_INDEX;
    }

    public boolean isRangeIndex() {
        return this == RANGE_INT_INDEX || this == RANGE_FLOAT_INDEX ||
               this == RANGE_LONG_INDEX || this == RANGE_DOUBLE_INDEX;
    }

    public boolean isRange4Index() {
        return this == RANGE_INT_INDEX || this == RANGE_FLOAT_INDEX;
    }

    public boolean isRange8Index() {
        return this == RANGE_LONG_INDEX || this == RANGE_DOUBLE_INDEX;
    }

    public boolean isShardIndex() {
        return this == SHARD_INDEX;
    }

    public boolean isUniqueIndex() {
        return this == UNIQUE_INDEX;
    }

    public boolean isVertexAggregateProperty() {
        return this == AGGR_PROPERTY_V;
    }

    public boolean isEdgeAggregateProperty() {
        return this == AGGR_PROPERTY_E;
    }

    public boolean isAggregateProperty() {
        return this.isVertexAggregateProperty() ||
               this.isEdgeAggregateProperty();
    }

    public static VortexType fromString(String type) {
        return ALL_NAME.get(type);
    }

    public static VortexType fromCode(byte code) {
        return SerialEnum.fromCode(VortexType.class, code);
    }
}
