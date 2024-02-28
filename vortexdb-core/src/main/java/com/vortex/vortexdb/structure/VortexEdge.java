
package com.vortex.vortexdb.structure;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.EdgeId;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.common.perf.PerfUtil.Watched;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.Cardinality;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.util.Strings;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class VortexEdge extends VortexElement implements Edge, Cloneable {

    private Id id;
    private EdgeLabel label;
    private String name;

    private VortexVertex sourceVertex;
    private VortexVertex targetVertex;
    private boolean isOutEdge;

    public VortexEdge(VortexVertex owner, Id id, EdgeLabel label,
                      VortexVertex other) {
        this(owner.graph(), id, label);
        this.fresh(true);
        this.vertices(owner, other);
    }

    public VortexEdge(final Vortex graph, Id id, EdgeLabel label) {
        super(graph);

        E.checkArgumentNotNull(label, "Edge label can't be null");
        this.label = label;

        this.id = id;
        this.name = null;
        this.sourceVertex = null;
        this.targetVertex = null;
        this.isOutEdge = true;
    }

    @Override
    public VortexType type() {
        // NOTE: we optimize the edge type that let it include direction
        return this.isOutEdge ? VortexType.EDGE_OUT : VortexType.EDGE_IN;
    }

    @Override
    public EdgeId id() {
        return (EdgeId) this.id;
    }

    @Override
    public EdgeLabel schemaLabel() {
        assert this.graph().sameAs(this.label.graph());
        return this.label;
    }

    @Override
    public String name() {
        if (this.name == null) {
            List<Object> sortValues = this.sortValues();
            if (sortValues.isEmpty()) {
                this.name = Strings.EMPTY;
            } else {
                this.name = ConditionQuery.concatValues(sortValues);
            }
        }
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }

    @Override
    public String label() {
        return this.label.name();
    }

    public boolean selfLoop() {
        return this.sourceVertex != null &&
               this.sourceVertex == this.targetVertex;
    }

    public Directions direction() {
        return this.isOutEdge ? Directions.OUT : Directions.IN;
    }

    public boolean matchDirection(Directions direction) {
        if (direction == Directions.BOTH || this.selfLoop()) {
            return true;
        }
        return this.isDirection(direction);
    }

    public boolean isDirection(Directions direction) {
        return this.isOutEdge && direction == Directions.OUT ||
               !this.isOutEdge && direction == Directions.IN;
    }

    @Watched(prefix = "edge")
    public void assignId() {
        // Generate an id and assign
        this.id = new EdgeId(this.ownerVertex(), this.direction(),
                             this.schemaLabel().id(), this.name(),
                             this.otherVertex());

        if (this.fresh()) {
            int len = this.id.length();
            E.checkArgument(len <= BytesBuffer.BIG_ID_LEN_MAX,
                            "The max length of edge id is %s, but got %s {%s}",
                            BytesBuffer.BIG_ID_LEN_MAX, len, this.id);
        }
    }

    @Watched(prefix = "edge")
    public EdgeId idWithDirection() {
        return ((EdgeId) this.id).directed(true);
    }

    @Watched(prefix = "edge")
    protected List<Object> sortValues() {
        List<Id> sortKeys = this.schemaLabel().sortKeys();
        if (sortKeys.isEmpty()) {
            return ImmutableList.of();
        }
        List<Object> propValues = new ArrayList<>(sortKeys.size());
        for (Id sk : sortKeys) {
            VortexProperty<?> property = this.getProperty(sk);
            E.checkState(property != null,
                         "The value of sort key '%s' can't be null", sk);
            Object propValue = property.serialValue(true);
            if (Strings.EMPTY.equals(propValue)) {
                propValue = ConditionQuery.INDEX_VALUE_EMPTY;
            }
            propValues.add(propValue);
        }
        return propValues;
    }

    @Watched(prefix = "edge")
    @Override
    public void remove() {
        this.removed(true);
        this.sourceVertex.removeEdge(this);
        this.targetVertex.removeEdge(this);

        GraphTransaction tx = this.tx();
        if (tx != null) {
            assert this.fresh();
            tx.removeEdge(this);
        } else {
            this.graph().removeEdge(this);
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        PropertyKey propertyKey = this.graph().propertyKey(key);
        // Check key in edge label
        E.checkArgument(this.label.properties().contains(propertyKey.id()),
                        "Invalid property '%s' for edge label '%s'",
                        key, this.label());
        // Sort-Keys can only be set once
        if (this.schemaLabel().sortKeys().contains(propertyKey.id())) {
            E.checkArgument(!this.hasProperty(propertyKey.id()),
                            "Can't update sort key: '%s'", key);
        }
        return this.addProperty(propertyKey, value, !this.fresh());
    }

    @Override
    protected GraphTransaction tx() {
        if (this.ownerVertex() == null || !this.fresh()) {
            return null;
        }
        return this.ownerVertex().tx();
    }

    @Watched(prefix = "edge")
    @Override
    protected <V> VortexEdgeProperty<V> newProperty(PropertyKey pkey, V val) {
        return new VortexEdgeProperty<>(this, pkey, val);
    }

    @Watched(prefix = "edge")
    @Override
    protected <V> void onUpdateProperty(Cardinality cardinality,
                                        VortexProperty<V> prop) {
        if (prop != null) {
            assert prop instanceof VortexEdgeProperty;
            VortexEdgeProperty<V> edgeProp = (VortexEdgeProperty<V>) prop;
            GraphTransaction tx = this.tx();
            if (tx != null) {
                assert this.fresh();
                tx.addEdgeProperty(edgeProp);
            } else {
                this.graph().addEdgeProperty(edgeProp);
            }
        }
    }

    @Watched(prefix = "edge")
    @Override
    protected boolean ensureFilledProperties(boolean throwIfNotExist) {
        if (this.isPropLoaded()) {
            this.updateToDefaultValueIfNone();
            return true;
        }

        // Skip query if there is no any property key in schema
        if (this.schemaLabel().properties().isEmpty()) {
            this.propLoaded();
            return true;
        }

        // Seems there is no scene to be here
        Iterator<Edge> edges = this.graph().edges(this.id());
        Edge edge = QueryResults.one(edges);
        if (edge == null && !throwIfNotExist) {
            return false;
        }
        E.checkState(edge != null, "Edge '%s' does not exist", this.id);
        this.copyProperties((VortexEdge) edge);
        this.updateToDefaultValueIfNone();
        return true;
    }

    @Watched(prefix = "edge")
    @SuppressWarnings("unchecked") // (Property<V>) prop
    @Override
    public <V> Iterator<Property<V>> properties(String... keys) {
        this.ensureFilledProperties(true);

        // Capacity should be about the following size
        int propsCapacity = keys.length == 0 ?
                            this.sizeOfProperties() :
                            keys.length;
        List<Property<V>> props = new ArrayList<>(propsCapacity);

        if (keys.length == 0) {
            for (VortexProperty<?> prop : this.getProperties()) {
                assert prop instanceof Property;
                props.add((Property<V>) prop);
            }
        } else {
            for (String key : keys) {
                Id pkeyId;
                try {
                    pkeyId = this.graph().propertyKey(key).id();
                } catch (IllegalArgumentException ignored) {
                    continue;
                }
                VortexProperty<?> prop = this.getProperty(pkeyId);
                if (prop == null) {
                    // Not found
                    continue;
                }
                assert prop instanceof Property;
                props.add((Property<V>) prop);
            }
        }
        return props.iterator();
    }

    @Override
    public Object sysprop(VortexKeys key) {
        switch (key) {
            case ID:
                return this.id();
            case OWNER_VERTEX:
                return this.ownerVertex().id();
            case LABEL:
                return this.schemaLabel().id();
            case DIRECTION:
                return this.direction();
            case OTHER_VERTEX:
                return this.otherVertex().id();
            case SORT_VALUES:
                return this.name();
            case PROPERTIES:
                return this.getPropertiesMap();
            default:
                E.checkArgument(false,
                                "Invalid system property '%s' of Edge", key);
                return null;
        }
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        List<Vertex> vertices = new ArrayList<>(2);
        switch (direction) {
            case OUT:
                vertices.add(this.sourceVertex());
                break;
            case IN:
                vertices.add(this.targetVertex());
                break;
            case BOTH:
                vertices.add(this.sourceVertex());
                vertices.add(this.targetVertex());
                break;
            default:
                throw new AssertionError("Unsupported direction: " + direction);
        }

        return vertices.iterator();
    }

    @Override
    public Vertex outVertex() {
        return this.sourceVertex();
    }

    @Override
    public Vertex inVertex() {
        return this.targetVertex();
    }

    public void vertices(VortexVertex owner, VortexVertex other) {
        Id ownerLabel = owner.schemaLabel().id();
        if (ownerLabel.equals(this.label.sourceLabel())) {
            this.vertices(true, owner, other);
        } else {
            ownerLabel.equals(this.label.targetLabel());
            this.vertices(false, owner, other);
        }
    }

    public void vertices(boolean outEdge, VortexVertex owner, VortexVertex other) {
        this.isOutEdge = outEdge;
        if (this.isOutEdge) {
            this.sourceVertex = owner;
            this.targetVertex = other;
        } else {
            this.sourceVertex = other;
            this.targetVertex = owner;
        }
    }

    @Watched
    public VortexEdge switchOwner() {
        VortexEdge edge = this.clone();
        edge.isOutEdge = !edge.isOutEdge;
        edge.id = ((EdgeId) edge.id).switchDirection();
        return edge;
    }

    public VortexEdge switchToOutDirection() {
        if (this.direction() == Directions.IN) {
            return this.switchOwner();
        }
        return this;
    }

    public VortexVertex ownerVertex() {
        return this.isOutEdge ? this.sourceVertex() : this.targetVertex();
    }

    public VortexVertex sourceVertex() {
        this.checkAdjacentVertexExist(this.sourceVertex);
        return this.sourceVertex;
    }

    public void sourceVertex(VortexVertex sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    public VortexVertex targetVertex() {
        this.checkAdjacentVertexExist(this.targetVertex);
        return this.targetVertex;
    }

    public void targetVertex(VortexVertex targetVertex) {
        this.targetVertex = targetVertex;
    }

    private void checkAdjacentVertexExist(VortexVertex vertex) {
        if (vertex.schemaLabel().undefined() &&
            this.graph().checkAdjacentVertexExist()) {
            throw new VortexException("Vertex '%s' does not exist", vertex.id());
        }
    }

    public boolean belongToLabels(String... edgeLabels) {
        if (edgeLabels.length == 0) {
            return true;
        }

        // Does edgeLabels contain me
        for (String label : edgeLabels) {
            if (label.equals(this.label())) {
                return true;
            }
        }
        return false;
    }

    public boolean belongToVertex(VortexVertex vertex) {
        return vertex != null && (vertex.equals(this.sourceVertex) ||
                                  vertex.equals(this.targetVertex));
    }

    public VortexVertex otherVertex(VortexVertex vertex) {
        if (vertex == this.sourceVertex()) {
            return this.targetVertex();
        } else {
            E.checkArgument(vertex == this.targetVertex(),
                            "Invalid argument vertex '%s', must be in [%s, %s]",
                            vertex, this.sourceVertex(), this.targetVertex());
            return this.sourceVertex();
        }
    }

    public VortexVertex otherVertex() {
        return this.isOutEdge ? this.targetVertex() : this.sourceVertex();
    }

    /**
     * Clear properties of the edge, and set `removed` true
     * @return a new edge
     */
    public VortexEdge prepareRemoved() {
        VortexEdge edge = this.clone();
        edge.removed(true);
        edge.resetProperties();
        return edge;
    }

    @Override
    public VortexEdge copy() {
        VortexEdge edge = this.clone();
        edge.copyProperties(this);
        return edge;
    }

    @Override
    protected VortexEdge clone() {
        try {
            return (VortexEdge) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new VortexException("Failed to clone VortexEdge", e);
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    public static final EdgeId getIdValue(Object idValue,
                                          boolean returnNullIfError) {
        Id id = VortexElement.getIdValue(idValue);
        if (id == null || id instanceof EdgeId) {
            return (EdgeId) id;
        }
        return EdgeId.parse(id.asString(), returnNullIfError);
    }

    @Watched
    public static VortexEdge constructEdge(VortexVertex ownerVertex,
                                           boolean isOutEdge,
                                           EdgeLabel edgeLabel,
                                           String sortValues,
                                           Id otherVertexId) {
        Vortex graph = ownerVertex.graph();
        VertexLabel srcLabel = graph.vertexLabelOrNone(edgeLabel.sourceLabel());
        VertexLabel tgtLabel = graph.vertexLabelOrNone(edgeLabel.targetLabel());

        VertexLabel otherVertexLabel;
        if (isOutEdge) {
            ownerVertex.correctVertexLabel(srcLabel);
            otherVertexLabel = tgtLabel;
        } else {
            ownerVertex.correctVertexLabel(tgtLabel);
            otherVertexLabel = srcLabel;
        }
        VortexVertex otherVertex = new VortexVertex(graph, otherVertexId,
                                                otherVertexLabel);

        ownerVertex.propNotLoaded();
        otherVertex.propNotLoaded();

        VortexEdge edge = new VortexEdge(graph, null, edgeLabel);
        edge.name(sortValues);
        edge.vertices(isOutEdge, ownerVertex, otherVertex);
        edge.assignId();

        if (isOutEdge) {
            ownerVertex.addOutEdge(edge);
            otherVertex.addInEdge(edge.switchOwner());
        } else {
            ownerVertex.addInEdge(edge);
            otherVertex.addOutEdge(edge.switchOwner());
        }

        return edge;
    }
}
