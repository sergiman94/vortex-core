
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.auth.SchemaDefine.Relationship;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.schema.EdgeLabel;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VortexBelong extends Relationship {

    private static final long serialVersionUID = -7242751631755533423L;

    private final Id user;
    private final Id group;
    private String description;

    public VortexBelong(Id user, Id group) {
        this.user = user;
        this.group = group;
        this.description = null;
    }

    @Override
    public ResourceType type() {
        return ResourceType.GRANT;
    }

    @Override
    public String label() {
        return P.BELONG;
    }

    @Override
    public String sourceLabel() {
        return P.USER;
    }

    @Override
    public String targetLabel() {
        return P.GROUP;
    }

    @Override
    public Id source() {
        return this.user;
    }

    @Override
    public Id target() {
        return this.group;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("VortexBelong(%s->%s)%s",
                             this.user, this.group, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        List<Object> list = new ArrayList<>(10);

        list.add(T.label);
        list.add(P.BELONG);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.USER), this.user);
        map.put(Hidden.unHide(P.GROUP), this.group);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static VortexBelong fromEdge(Edge edge) {
        VortexBelong belong = new VortexBelong((Id) edge.outVertex().id(),
                                           (Id) edge.inVertex().id());
        return fromEdge(edge, belong);
    }

    public static Schema schema(VortexParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String BELONG = Hidden.hide("belong");

        public static final String LABEL = T.label.getAccessor();

        public static final String USER = VortexUser.P.USER;
        public static final String GROUP = VortexGroup.P.GROUP;

        public static final String DESCRIPTION = "~belong_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("belong_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(VortexParams graph) {
            super(graph, P.BELONG);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create edge label
            EdgeLabel label = this.schema().edgeLabel(this.label)
                                  .sourceLabel(P.USER)
                                  .targetLabel(P.GROUP)
                                  .properties(properties)
                                  .nullableKeys(P.DESCRIPTION)
                                  .enableLabelIndex(true)
                                  .build();
            this.graph.schemaTransaction().addEdgeLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }
}
