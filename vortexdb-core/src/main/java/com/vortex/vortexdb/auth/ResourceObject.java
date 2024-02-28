
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.auth.SchemaDefine.AuthElement;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.type.Namifiable;
import com.vortex.common.util.E;

public class ResourceObject<V> {

    private final String graph;
    private final ResourceType type;
    private final V operated;

    public ResourceObject(String graph, ResourceType type, V operated) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(type, "type");
        E.checkNotNull(operated, "operated");
        this.graph = graph;
        this.type = type;
        this.operated = operated;
    }

    public String graph() {
        return this.graph;
    }

    public ResourceType type() {
        return this.type;
    }

    public V operated() {
        return this.operated;
    }

    @Override
    public String toString() {
        Object operated = this.operated;
        if (this.type.isAuth()) {
            operated = ((AuthElement) this.operated).idString();
        }

        String typeStr = this.type.toString();
        String operatedStr = operated.toString();
        int capacity = this.graph.length() + typeStr.length() +
                       operatedStr.length() + 36;

        StringBuilder sb = new StringBuilder(capacity);
        return sb.append("Resource{graph=").append(this.graph)
                 .append(",type=").append(typeStr)
                 .append(",operated=").append(operatedStr)
                 .append("}").toString();
    }

    public static ResourceObject<SchemaElement> of(String graph,
                                                   SchemaElement elem) {
        ResourceType resType = ResourceType.from(elem.type());
        return new ResourceObject<>(graph, resType, elem);
    }

    public static ResourceObject<VortexElement> of(String graph,
                                                 VortexElement elem) {
        ResourceType resType = ResourceType.from(elem.type());
        return new ResourceObject<>(graph, resType, elem);
    }

    public static ResourceObject<AuthElement> of(String graph,
                                                 AuthElement elem) {
        return new ResourceObject<>(graph, elem.type(), elem);
    }

    public static ResourceObject<?> of(String graph, ResourceType type,
                                       Namifiable elem) {
        return new ResourceObject<>(graph, type, elem);
    }
}
