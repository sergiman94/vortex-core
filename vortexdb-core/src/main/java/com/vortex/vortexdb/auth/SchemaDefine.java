package com.vortex.vortexdb.auth;

import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.auth.VortexTarget.P;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.SchemaManager;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.Cardinality;
import com.vortex.vortexdb.type.define.DataType;
import com.vortex.common.util.E;


public abstract class SchemaDefine {

    protected final VortexParams graph;
    protected final String label;

    public SchemaDefine(VortexParams graph, String label) {
        this.graph = graph;
        this.label = label;
    }

    public abstract void initSchemaIfNeeded();

    protected SchemaManager schema() {
         return this.graph.graph().schema();
    }

    protected boolean existVertexLabel(String label) {
        return this.graph.graph().existsVertexLabel(label);
    }

    protected boolean existEdgeLabel(String label) {
        return this.graph.graph().existsEdgeLabel(label);
    }

    protected String createPropertyKey(String name) {
        return this.createPropertyKey(name, DataType.TEXT);
    }

    protected String createPropertyKey(String name, DataType dataType) {
        return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    protected String createPropertyKey(String name, DataType dataType,
                                       Cardinality cardinality) {
        SchemaManager schema = this.schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        this.graph.schemaTransaction().addPropertyKey(propertyKey);
        return name;
    }

    protected String[] initProperties(List<String> props) {
        String label = this.label;
        props.add(createPropertyKey(hideField(label, AuthElement.CREATE),
                                    DataType.DATE));
        props.add(createPropertyKey(hideField(label, AuthElement.UPDATE),
                                    DataType.DATE));
        props.add(createPropertyKey(hideField(label, AuthElement.CREATOR)));

        return props.toArray(new String[0]);
    }

    protected IndexLabel createRangeIndex(VertexLabel label, String field) {
        SchemaManager schema = this.schema();
        String name = Hidden.hide(label + "-index-by-" + field);
        IndexLabel indexLabel = schema.indexLabel(name).range()
                                      .on(VortexType.VERTEX_LABEL, this.label)
                                      .by(field)
                                      .build();
        this.graph.schemaTransaction().addIndexLabel(label, indexLabel);
        return indexLabel;
    }

    protected static String hideField(String label, String key) {
        return label + "_" + key;
    }

    protected static String unhideField(String label, String key) {
        return Hidden.unHide(label) + "_" + key;
    }

    public static abstract class AuthElement implements Serializable {

        private static final long serialVersionUID = 8746691160192814973L;

        protected static final String CREATE = "create";
        protected static final String UPDATE = "update";
        protected static final String CREATOR = "creator";

        protected Id id;
        protected Date create;
        protected Date update;
        protected String creator;

        public AuthElement() {
            this.create = new Date();
            this.update = this.create;
        }

        public Id id() {
            return this.id;
        }

        public void id(Id id) {
            this.id = id;
        }

        public String idString() {
            return Hidden.unHide(this.label()) + "(" + this.id + ")";
        }

        public Date create() {
            return this.create;
        }

        public void create(Date create) {
            this.create = create;
        }

        public Date update() {
            return this.update;
        }

        public void update(Date update) {
            this.update = update;
        }

        public void onUpdate() {
            this.update = new Date();
        }

        public String creator() {
            return this.creator;
        }

        public void creator(String creator) {
            this.creator = creator;
        }

        protected Map<String, Object> asMap(Map<String, Object> map) {
            E.checkState(this.create != null,
                         "Property %s time can't be null", CREATE);
            E.checkState(this.update != null,
                         "Property %s time can't be null", UPDATE);
            E.checkState(this.creator != null,
                         "Property %s can't be null", CREATOR);

            if (this.id != null) {
                // The id is null when creating
                map.put(Hidden.unHide(P.ID), this.id);
            }

            map.put(unhideField(this.label(), CREATE), this.create);
            map.put(unhideField(this.label(), UPDATE), this.update);
            map.put(unhideField(this.label(), CREATOR), this.creator);

            return map;
        }

        protected boolean property(String key, Object value) {
            E.checkNotNull(key, "property key");
            if (key.equals(hideField(this.label(), CREATE))) {
                this.create = (Date) value;
                return true;
            }
            if (key.equals(hideField(this.label(), UPDATE))) {
                this.update = (Date) value;
                return true;
            }
            if (key.equals(hideField(this.label(), CREATOR))) {
                this.creator = (String) value;
                return true;
            }
            return false;
        }

        protected Object[] asArray(List<Object> list) {
            E.checkState(this.create != null,
                         "Property %s time can't be null", CREATE);
            E.checkState(this.update != null,
                         "Property %s time can't be null", UPDATE);
            E.checkState(this.creator != null,
                         "Property %s can't be null", CREATOR);

            list.add(hideField(this.label(), CREATE));
            list.add(this.create);

            list.add(hideField(this.label(), UPDATE));
            list.add(this.update);

            list.add(hideField(this.label(), CREATOR));
            list.add(this.creator);

            return list.toArray();
        }

        public abstract ResourceType type();

        public abstract String label();

        public abstract Map<String, Object> asMap();

        protected abstract Object[] asArray();
    }

    // NOTE: travis-ci fails if class Entity implements Namifiable
    public static abstract class Entity extends AuthElement
                           implements com.vortex.vortexdb.type.Namifiable {

        private static final long serialVersionUID = 4113319546914811762L;

        public static <T extends Entity> T fromVertex(Vertex vertex, T entity) {
            E.checkArgument(vertex.label().equals(entity.label()),
                            "Illegal vertex label '%s' for entity '%s'",
                            vertex.label(), entity.label());
            entity.id((Id) vertex.id());
            for (Iterator<VertexProperty<Object>> iter = vertex.properties();
                 iter.hasNext();) {
                VertexProperty<Object> prop = iter.next();
                entity.property(prop.key(), prop.value());
            }
            return entity;
        }

        @Override
        public String idString() {
            String label = Hidden.unHide(this.label());
            String name = this.name();
            StringBuilder sb = new StringBuilder(label.length() +
                                                 name.length() + 2);
            sb.append(label).append("(").append(name).append(")");
            return sb.toString();
        }
    }

    public static abstract class Relationship extends AuthElement {

        private static final long serialVersionUID = -1406157381685832493L;

        public abstract String sourceLabel();
        public abstract String targetLabel();

        public abstract Id source();
        public abstract Id target();

        public static <T extends Relationship> T fromEdge(Edge edge,
                                                          T relationship) {
            E.checkArgument(edge.label().equals(relationship.label()),
                            "Illegal edge label '%s' for relationship '%s'",
                            edge.label(), relationship.label());
            relationship.id((Id) edge.id());
            for (Iterator<Property<Object>> iter = edge.properties();
                 iter.hasNext();) {
                Property<Object> prop = iter.next();
                relationship.property(prop.key(), prop.value());
            }
            return relationship;
        }

        @Override
        public String idString() {
            String label = Hidden.unHide(this.label());
            StringBuilder sb = new StringBuilder(label.length() +
                                                 this.source().length() +
                                                 this.target().length() + 4);
            sb.append(label).append("(").append(this.source())
              .append("->").append(this.target()).append(")");
            return sb.toString();
        }
    }
}
