
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.auth.SchemaDefine.Relationship;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.Condition;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.common.iterator.MapperIterator;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class RelationshipManager<T extends SchemaDefine.Relationship> {

    private final VortexParams graph;
    private final String label;
    private final Function<Edge, T> deser;
    private final ThreadLocal<Boolean> autoCommit = new ThreadLocal<>();

    private static final long NO_LIMIT = -1L;

    public RelationshipManager(VortexParams graph, String label,
                               Function<Edge, T> deser) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.label = label;
        this.deser = deser;
        this.autoCommit.set(true);
    }

    private GraphTransaction tx() {
        return this.graph.systemTransaction();
    }

    private Vortex graph() {
        return this.graph.graph();
    }

    private String unhideLabel() {
        return Hidden.unHide(this.label) ;
    }

    public Id add(T relationship) {
        E.checkArgumentNotNull(relationship, "Relationship can't be null");
        return this.save(relationship, false);
    }

    public Id update(T relationship) {
        E.checkArgumentNotNull(relationship, "Relationship can't be null");
        relationship.onUpdate();
        return this.save(relationship, true);
    }

    public T delete(Id id) {
        T relationship = null;
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            VortexEdge edge = (VortexEdge) edges.next();
            relationship = this.deser.apply(edge);
            this.tx().removeEdge(edge);
            this.commitOrRollback();
            assert !edges.hasNext();
        }
        return relationship;
    }

    public T get(Id id) {
        T relationship = null;
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            relationship = this.deser.apply(edges.next());
            assert !edges.hasNext();
        }
        if (relationship == null) {
            throw new NotFoundException("Can't find %s with id '%s'",
                                        this.unhideLabel(), id);
        }
        return relationship;
    }

    public boolean exists(Id id) {
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            Edge edge = edges.next();
            if (this.label.equals(edge.label())) {
                return true;
            }
        }
        return false;
    }

    public List<T> list(List<Id> ids) {
        return toList(this.queryById(ids));
    }

    public List<T> list(long limit) {
        Iterator<Edge> edges = this.queryRelationship(null, null, this.label,
                                                      ImmutableMap.of(), limit);
        return toList(edges);
    }

    public List<T> list(Id source, Directions direction,
                        String label, long limit) {
        Iterator<Edge> edges = this.queryRelationship(source, direction, label,
                                                      ImmutableMap.of(), limit);
        return toList(edges);
    }

    public List<T> list(Id source, Directions direction, String label,
                        String key, Object value, long limit) {
        Map<String, Object> conditions = ImmutableMap.of(key, value);
        Iterator<Edge> edges = this.queryRelationship(source, direction, label,
                                                      conditions, limit);
        return toList(edges);
    }

    protected List<T> toList(Iterator<Edge> edges) {
        Iterator<T> iter = new MapperIterator<>(edges, this.deser);
        // Convert iterator to list to avoid across thread tx accessed
        return (List<T>) QueryResults.toList(iter).list();
    }

    private Iterator<Edge> queryById(List<Id> ids) {
        Object[] idArray = ids.toArray(new Id[ids.size()]);
        return this.tx().queryEdges(idArray);
    }

    private Iterator<Edge> queryRelationship(Id source,
                                             Directions direction,
                                             String label,
                                             Map<String, Object> conditions,
                                             long limit) {
        ConditionQuery query = new ConditionQuery(VortexType.EDGE);
        EdgeLabel el = this.graph().edgeLabel(label);
        if (direction == null) {
            direction = Directions.OUT;
        }
        if (source != null) {
            query.eq(VortexKeys.OWNER_VERTEX, source);
            query.eq(VortexKeys.DIRECTION, direction);
        }
        if (label != null) {
            query.eq(VortexKeys.LABEL, el.id());
        }
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pk = this.graph().propertyKey(entry.getKey());
            query.query(Condition.eq(pk.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        Iterator<Edge> edges = this.tx().queryEdges(query);
        if (limit == NO_LIMIT) {
            return edges;
        }
        long[] size = new long[1];
        return new MapperIterator<>(edges, edge -> {
            if (++size[0] > limit) {
                return null;
            }
            return edge;
        });
    }

    private Id save(T relationship, boolean expectExists) {
        if (!this.graph().existsEdgeLabel(relationship.label())) {
            throw new VortexException("Schema is missing for %s '%s'",
                                    relationship.label(),
                                    relationship.source());
        }
        VortexVertex source = this.newVertex(relationship.source(),
                                           relationship.sourceLabel());
        VortexVertex target = this.newVertex(relationship.target(),
                                           relationship.targetLabel());
        VortexEdge edge = source.constructEdge(relationship.label(), target,
                                             relationship.asArray());
        E.checkArgument(this.exists(edge.id()) == expectExists,
                        "Can't save %s '%s' that %s exists",
                        this.unhideLabel(), edge.id(),
                        expectExists ? "not" : "already");

        this.tx().addEdge(edge);
        this.commitOrRollback();
        return edge.id();
    }

    private VortexVertex newVertex(Object id, String label) {
        VertexLabel vl = this.graph().vertexLabel(label);
        Id idValue = VortexVertex.getIdValue(id);
        return VortexVertex.create(this.tx(), idValue, vl);
    }

    private void commitOrRollback() {
        Boolean autoCommit = this.autoCommit.get();
        if (autoCommit != null && !autoCommit) {
            return;
        }
        this.tx().commitOrRollback();
    }

    public void autoCommit(boolean value) {
        autoCommit.set(value);
    }
}
