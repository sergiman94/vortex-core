
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.type.VortexType;

public enum ResourceType {

    NONE,

    STATUS,

    VERTEX,

    EDGE,

    VERTEX_AGGR,

    EDGE_AGGR,

    VAR,

    GREMLIN,

    TASK,

    PROPERTY_KEY,

    VERTEX_LABEL,

    EDGE_LABEL,

    INDEX_LABEL, // include create/rebuild/delete index

    SCHEMA,

    META,

    ALL,

    GRANT,

    USER_GROUP,

    PROJECT,

    TARGET,

    METRICS,

    ROOT;

    public boolean match(ResourceType required) {
        if (this == required) {
            return true;
        }

        switch (required) {
            case NONE:
                return this != NONE;
            default:
                break;
        }

        switch (this) {
            case ROOT:
            case ALL:
                return this.ordinal() >= required.ordinal();
            case SCHEMA:
                return required.isSchema();
            default:
                break;
        }

        return false;
    }

    public boolean isGraph() {
        int ord = this.ordinal();
        return VERTEX.ordinal() <= ord && ord <= EDGE.ordinal();
    }

    public boolean isSchema() {
        int ord = this.ordinal();
        return PROPERTY_KEY.ordinal() <= ord && ord <= SCHEMA.ordinal();
    }

    public boolean isAuth() {
        int ord = this.ordinal();
        return GRANT.ordinal() <= ord && ord <= TARGET.ordinal();
    }

    public boolean isGrantOrUser() {
        return this == GRANT || this == USER_GROUP;
    }

    public boolean isRepresentative() {
        return this == ROOT || this == ALL || this == SCHEMA;
    }

    public static ResourceType from(VortexType type) {
        switch (type) {
            case VERTEX:
                return VERTEX;
            case EDGE:
            case EDGE_OUT:
            case EDGE_IN:
                return EDGE;
            case PROPERTY_KEY:
                return PROPERTY_KEY;
            case VERTEX_LABEL:
                return VERTEX_LABEL;
            case EDGE_LABEL:
                return EDGE_LABEL;
            case INDEX_LABEL:
                return INDEX_LABEL;
            default:
                // pass
                break;
        }
        return NONE;
    }
}
