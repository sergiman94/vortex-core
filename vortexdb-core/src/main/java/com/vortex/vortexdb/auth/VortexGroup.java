
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.auth.SchemaDefine.Entity;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VortexGroup extends Entity {

    private static final long serialVersionUID = 2330399818352242686L;

    private String name;
    private String description;

    public VortexGroup(String name) {
        this(null, name);
    }

    public VortexGroup(Id id) {
        this(id, null);
    }

    public VortexGroup(Id id, String name) {
        this.id = id;
        this.name = name;
        this.description = null;
    }

    @Override
    public ResourceType type() {
        return ResourceType.USER_GROUP;
    }

    @Override
    public String label() {
        return P.GROUP;
    }

    @Override
    public String name() {
        return this.name;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("VortexGroup(%s)%s", this.id, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.NAME:
                this.name = (String) value;
                break;
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
        E.checkState(this.name != null, "Group name can't be null");

        List<Object> list = new ArrayList<>(12);

        list.add(T.label);
        list.add(P.GROUP);

        list.add(P.NAME);
        list.add(this.name);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "Group name can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.NAME), this.name);
        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static VortexGroup fromVertex(Vertex vertex) {
        VortexGroup group = new VortexGroup((Id) vertex.id());
        return fromVertex(vertex, group);
    }

    public static Schema schema(VortexParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String GROUP = Hidden.hide("group");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~group_name";
        public static final String DESCRIPTION = "~group_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("group_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(VortexParams graph) {
            super(graph, P.GROUP);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create vertex label
            VertexLabel label = this.schema().vertexLabel(this.label)
                                    .properties(properties)
                                    .usePrimaryKeyId()
                                    .primaryKeys(P.NAME)
                                    .nullableKeys(P.DESCRIPTION)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        protected String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }
}
