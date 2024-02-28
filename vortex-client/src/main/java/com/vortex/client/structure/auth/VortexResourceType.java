package com.vortex.client.structure.auth;

public enum VortexResourceType {

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
}
