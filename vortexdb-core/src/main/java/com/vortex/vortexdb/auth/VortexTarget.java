

package com.vortex.vortexdb.auth;

import com.google.common.collect.ImmutableList;
import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.auth.SchemaDefine.Entity;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VortexTarget extends Entity {

    private static final long serialVersionUID = -3361487778656878418L;

    private String name;
    private String graph;
    private String url;
    private List<VortexResource> resources;

    private static final List<VortexResource> EMPTY = ImmutableList.of();

    public VortexTarget(Id id) {
        this(id, null, null, null, EMPTY);
    }

    public VortexTarget(String name, String url) {
        this(null, name, name, url, EMPTY);
    }

    public VortexTarget(String name, String graph, String url) {
        this(null, name, graph, url, EMPTY);
    }

    public VortexTarget(String name, String graph, String url,
                        List<VortexResource> resources) {
        this(null, name, graph, url, resources);
    }

    private VortexTarget(Id id, String name, String graph, String url,
                         List<VortexResource> resources) {
        this.id = id;
        this.name = name;
        this.graph = graph;
        this.url = url;
        this.resources = resources;
    }

    @Override
    public ResourceType type() {
        return ResourceType.TARGET;
    }

    @Override
    public String label() {
        return P.TARGET;
    }

    @Override
    public String name() {
        return this.name;
    }

    public String graph() {
        return this.graph;
    }

    public String url() {
        return this.url;
    }

    public void url(String url) {
        this.url = url;
    }

    public List<VortexResource> resources() {
        return this.resources;
    }

    public void resources(String resources) {
        try {
            this.resources = VortexResource.parseResources(resources);
        } catch (Exception e) {
            throw new VortexException("Invalid format of resources: %s",
                                    e, resources);
        }
    }

    public void resources(List<VortexResource> resources) {
        E.checkNotNull(resources, "resources");
        this.resources = resources;
    }

    @Override
    public String toString() {
        return String.format("VortexTarget(%s)%s", this.id, this.asMap());
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
            case P.GRAPH:
                this.graph = (String) value;
                break;
            case P.URL:
                this.url = (String) value;
                break;
            case P.RESS:
                this.resources = VortexResource.parseResources((String) value);
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");

        List<Object> list = new ArrayList<>(16);

        list.add(T.label);
        list.add(P.TARGET);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.GRAPH);
        list.add(this.graph);

        list.add(P.URL);
        list.add(this.url);

        if (this.resources != null && this.resources != EMPTY) {
            list.add(P.RESS);
            list.add(JsonUtil.toJson(this.resources));
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.GRAPH), this.graph);
        map.put(Hidden.unHide(P.URL), this.url);

        if (this.resources != null && this.resources != EMPTY) {
            map.put(Hidden.unHide(P.RESS), this.resources);
        }

        return super.asMap(map);
    }

    public static VortexTarget fromVertex(Vertex vertex) {
        VortexTarget target = new VortexTarget((Id) vertex.id());
        return fromVertex(vertex, target);
    }

    public static Schema schema(VortexParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String TARGET = Hidden.hide("target");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~target_name";
        public static final String GRAPH = "~target_graph";
        public static final String URL = "~target_url";
        public static final String RESS = "~target_resources";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("target_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(VortexParams graph) {
            super(graph, P.TARGET);
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
                                    .nullableKeys(P.RESS)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.GRAPH));
            props.add(createPropertyKey(P.URL));
            props.add(createPropertyKey(P.RESS));

            return super.initProperties(props);
        }
    }
}
