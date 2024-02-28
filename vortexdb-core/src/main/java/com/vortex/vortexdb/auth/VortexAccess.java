
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.auth.SchemaDefine.Relationship;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.type.define.DataType;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VortexAccess extends Relationship {

    private static final long serialVersionUID = -7644007602408729385L;

    private final Id group;
    private final Id target;
    private VortexPermission permission;
    private String description;

    public VortexAccess(Id group, Id target) {
        this(group, target, null);
    }

    public VortexAccess(Id group, Id target, VortexPermission permission) {
        this.group = group;
        this.target = target;
        this.permission = permission;
        this.description = null;
    }

    @Override
    public ResourceType type() {
        return ResourceType.GRANT;
    }

    @Override
    public String label() {
        return P.ACCESS;
    }

    @Override
    public String sourceLabel() {
        return P.GROUP;
    }

    @Override
    public String targetLabel() {
        return P.TARGET;
    }

    @Override
    public Id source() {
        return this.group;
    }

    @Override
    public Id target() {
        return this.target;
    }

    public VortexPermission permission() {
        return this.permission;
    }

    public void permission(VortexPermission permission) {
        this.permission = permission;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("VortexAccess(%s->%s)%s",
                             this.group, this.target, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.PERMISSION:
                this.permission = VortexPermission.fromCode((Byte) value);
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
        E.checkState(this.permission != null,
                     "Access permission can't be null");

        List<Object> list = new ArrayList<>(12);

        list.add(T.label);
        list.add(P.ACCESS);

        list.add(P.PERMISSION);
        list.add(this.permission.code());

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.permission != null,
                     "Access permission can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.GROUP), this.group);
        map.put(Hidden.unHide(P.TARGET), this.target);

        map.put(Hidden.unHide(P.PERMISSION), this.permission);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static VortexAccess fromEdge(Edge edge) {
        VortexAccess access = new VortexAccess((Id) edge.outVertex().id(),
                                           (Id) edge.inVertex().id());
        return fromEdge(edge, access);
    }

    public static Schema schema(VortexParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String ACCESS = Hidden.hide("access");

        public static final String LABEL = T.label.getAccessor();

        public static final String GROUP = VortexGroup.P.GROUP;
        public static final String TARGET = VortexTarget.P.TARGET;

        public static final String PERMISSION = "~access_permission";
        public static final String DESCRIPTION = "~access_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("access_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(VortexParams graph) {
            super(graph, P.ACCESS);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create edge label
            EdgeLabel label = this.schema().edgeLabel(this.label)
                                  .sourceLabel(P.GROUP)
                                  .targetLabel(P.TARGET)
                                  .properties(properties)
                                  .nullableKeys(P.DESCRIPTION)
                                  .sortKeys(P.PERMISSION)
                                  .enableLabelIndex(true)
                                  .build();
            this.graph.schemaTransaction().addEdgeLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.PERMISSION, DataType.BYTE));
            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }
}
