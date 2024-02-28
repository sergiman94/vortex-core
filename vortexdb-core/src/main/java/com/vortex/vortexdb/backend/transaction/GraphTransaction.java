
package com.vortex.vortexdb.backend.transaction;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.EdgeId;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.SplicingIdGenerator;
import com.vortex.vortexdb.backend.page.IdHolderList;
import com.vortex.vortexdb.backend.page.PageInfo;
import com.vortex.vortexdb.backend.page.QueryList;
import com.vortex.vortexdb.backend.query.*;
import com.vortex.vortexdb.backend.query.Aggregate.AggregateFunc;
import com.vortex.vortexdb.backend.query.ConditionQuery.OptimizedType;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendMutation;
import com.vortex.vortexdb.backend.store.BackendStore;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.vortexdb.exception.LimitExceedException;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.common.iterator.ListIterator;
import com.vortex.common.iterator.*;
import com.vortex.vortexdb.job.system.DeleteExpiredJob;
import com.vortex.common.perf.PerfUtil.Watched;
import com.vortex.vortexdb.schema.*;
import com.vortex.vortexdb.structure.*;
import com.vortex.vortexdb.structure.VortexFeatures.VortexVertexFeatures;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.Action;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.vortexdb.type.define.IdStrategy;
import com.vortex.common.util.E;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.vortexdb.util.LockUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import javax.ws.rs.ForbiddenException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class GraphTransaction extends IndexableTransaction {

    public static final int COMMIT_BATCH = (int) Query.COMMIT_BATCH;

    private final GraphIndexTransaction indexTx;

    private Map<Id, VortexVertex> addedVertices;
    private Map<Id, VortexVertex> removedVertices;

    private Map<Id, VortexEdge> addedEdges;
    private Map<Id, VortexEdge> removedEdges;

    private Set<VortexProperty<?>> addedProps;
    private Set<VortexProperty<?>> removedProps;

    // These are used to rollback state
    private Map<Id, VortexVertex> updatedVertices;
    private Map<Id, VortexEdge> updatedEdges;
    private Set<VortexProperty<?>> updatedOldestProps; // Oldest props

    private LockUtil.LocksTable locksTable;

    private final boolean checkCustomVertexExist;
    private final boolean checkAdjacentVertexExist;
    private final boolean lazyLoadAdjacentVertex;
    private final boolean removeLeftIndexOnOverwrite;
    private final boolean ignoreInvalidEntry;
    private final boolean optimizeAggrByIndex;
    private final int commitPartOfAdjacentEdges;
    private final int batchSize;
    private final int pageSize;

    private final int verticesCapacity;
    private final int edgesCapacity;

    public GraphTransaction(VortexParams graph, BackendStore store) {
        super(graph, store);

        this.indexTx = new GraphIndexTransaction(graph, store);
        assert !this.indexTx.autoCommit();

        this.locksTable = new LockUtil.LocksTable(graph.name());

        final VortexConfig conf = graph.configuration();
        this.checkCustomVertexExist =
             conf.get(CoreOptions.VERTEX_CHECK_CUSTOMIZED_ID_EXIST);
        this.checkAdjacentVertexExist =
             conf.get(CoreOptions.VERTEX_ADJACENT_VERTEX_EXIST);
        this.lazyLoadAdjacentVertex =
             conf.get(CoreOptions.VERTEX_ADJACENT_VERTEX_LAZY);
        this.removeLeftIndexOnOverwrite =
             conf.get(CoreOptions.VERTEX_REMOVE_LEFT_INDEX);
        this.commitPartOfAdjacentEdges =
             conf.get(CoreOptions.VERTEX_PART_EDGE_COMMIT_SIZE);
        this.ignoreInvalidEntry =
             conf.get(CoreOptions.QUERY_IGNORE_INVALID_DATA);
        this.optimizeAggrByIndex =
             conf.get(CoreOptions.QUERY_OPTIMIZE_AGGR_BY_INDEX);
        this.batchSize = conf.get(CoreOptions.QUERY_BATCH_SIZE);
        this.pageSize = conf.get(CoreOptions.QUERY_PAGE_SIZE);

        this.verticesCapacity = conf.get(CoreOptions.VERTEX_TX_CAPACITY);
        this.edgesCapacity = conf.get(CoreOptions.EDGE_TX_CAPACITY);

        E.checkArgument(this.commitPartOfAdjacentEdges < this.edgesCapacity,
                        "Option value of %s(%s) must be < %s(%s)",
                        CoreOptions.VERTEX_PART_EDGE_COMMIT_SIZE.name(),
                        this.commitPartOfAdjacentEdges,
                        CoreOptions.EDGE_TX_CAPACITY.name(),
                        this.edgesCapacity);
    }

    @Override
    public boolean hasUpdate() {
        return this.mutationSize() > 0 || super.hasUpdate();
    }

    @Override
    public boolean hasUpdate(VortexType type, Action action) {
        if (type.isVertex()) {
            if (action == Action.DELETE) {
                if (this.removedVertices.size() > 0) {
                    return true;
                }
            } else {
                if (this.addedVertices.size() > 0 ||
                    this.updatedVertices.size() > 0) {
                    return true;
                }
            }
        } else if (type.isEdge()) {
            if (action == Action.DELETE) {
                if (this.removedEdges.size() > 0) {
                    return true;
                }
            } else {
                if (this.addedEdges.size() > 0 ||
                    this.updatedEdges.size() > 0) {
                    return true;
                }
            }
        }
        return super.hasUpdate(type, action);
    }

    @Override
    public int mutationSize() {
        return this.verticesInTxSize() + this.edgesInTxSize();
    }

    public boolean checkAdjacentVertexExist() {
        return this.checkAdjacentVertexExist;
    }

    @Override
    protected void reset() {
        super.reset();

        // Clear mutation
        if (this.addedVertices == null || !this.addedVertices.isEmpty()) {
            this.addedVertices = InsertionOrderUtil.newMap();
        }
        if (this.removedVertices == null || !this.removedVertices.isEmpty()) {
            this.removedVertices = InsertionOrderUtil.newMap();
        }
        if (this.updatedVertices == null || !this.updatedVertices.isEmpty()) {
            this.updatedVertices = InsertionOrderUtil.newMap();
        }

        if (this.addedEdges == null || !this.addedEdges.isEmpty()) {
            this.addedEdges = InsertionOrderUtil.newMap();
        }
        if (this.removedEdges == null || !this.removedEdges.isEmpty()) {
            this.removedEdges = InsertionOrderUtil.newMap();
        }
        if (this.updatedEdges == null || !this.updatedEdges.isEmpty()) {
            this.updatedEdges = InsertionOrderUtil.newMap();
        }

        if (this.addedProps == null || !this.addedProps.isEmpty()) {
            this.addedProps = InsertionOrderUtil.newSet();
        }
        if (this.removedProps == null || !this.removedProps.isEmpty()) {
            this.removedProps = InsertionOrderUtil.newSet();
        }
        if (this.updatedOldestProps == null ||
            !this.updatedOldestProps.isEmpty()) {
            this.updatedOldestProps = InsertionOrderUtil.newSet();
        }
    }

    @Override
    protected GraphIndexTransaction indexTransaction() {
        return this.indexTx;
    }

    @Override
    protected void beforeWrite() {
        this.checkTxVerticesCapacity();
        this.checkTxEdgesCapacity();

        super.beforeWrite();
    }

    protected final int verticesInTxSize() {
        return this.addedVertices.size() +
               this.removedVertices.size() +
               this.updatedVertices.size();
    }

    protected final int edgesInTxSize() {
        return this.addedEdges.size() +
               this.removedEdges.size() +
               this.updatedEdges.size();
    }

    protected final Collection<VortexVertex> verticesInTxUpdated() {
        int size = this.addedVertices.size() + this.updatedVertices.size();
        List<VortexVertex> vertices = new ArrayList<>(size);
        vertices.addAll(this.addedVertices.values());
        vertices.addAll(this.updatedVertices.values());
        return vertices;
    }

    protected final Collection<VortexVertex> verticesInTxRemoved() {
        return new ArrayList<>(this.removedVertices.values());
    }

    protected final boolean removingEdgeOwner(VortexEdge edge) {
        for (VortexVertex vertex : this.removedVertices.values()) {
            if (edge.belongToVertex(vertex)) {
                return true;
            }
        }
        return false;
    }

    @Watched(prefix = "tx")
    @Override
    protected BackendMutation prepareCommit() {
        // Serialize and add updates into super.deletions
        if (this.removedVertices.size() > 0 || this.removedEdges.size() > 0) {
            this.prepareDeletions(this.removedVertices, this.removedEdges);
        }

        if (this.addedProps.size() > 0 || this.removedProps.size() > 0) {
            this.prepareUpdates(this.addedProps, this.removedProps);
        }

        // Serialize and add updates into super.additions
        if (this.addedVertices.size() > 0 || this.addedEdges.size() > 0) {
            this.prepareAdditions(this.addedVertices, this.addedEdges);
        }

        return this.mutation();
    }

    protected void prepareAdditions(Map<Id, VortexVertex> addedVertices,
                                    Map<Id, VortexEdge> addedEdges) {
        if (this.checkCustomVertexExist) {
            this.checkVertexExistIfCustomizedId(addedVertices);
        }

        if (this.removeLeftIndexOnOverwrite) {
            this.removeLeftIndexIfNeeded(addedVertices);
        }

        // Do vertex update
        for (VortexVertex v : addedVertices.values()) {
            assert !v.removed();
            v.committed();
            this.checkAggregateProperty(v);
            // Check whether passed all non-null properties
            if (!this.graphMode().loading()) {
                this.checkNonnullProperty(v);
            }

            // Add vertex entry
            this.doInsert(this.serializer.writeVertex(v));
            // Update index of vertex(only include props)
            this.indexTx.updateVertexIndex(v, false);
            this.indexTx.updateLabelIndex(v, false);
        }

        // Do edge update
        for (VortexEdge e : addedEdges.values()) {
            assert !e.removed();
            e.committed();
            // Skip edge if its owner has been removed
            if (this.removingEdgeOwner(e)) {
                continue;
            }
            this.checkAggregateProperty(e);
            // Add edge entry of OUT and IN
            this.doInsert(this.serializer.writeEdge(e));
            this.doInsert(this.serializer.writeEdge(e.switchOwner()));
            // Update index of edge
            this.indexTx.updateEdgeIndex(e, false);
            this.indexTx.updateLabelIndex(e, false);
        }
    }

    protected void prepareDeletions(Map<Id, VortexVertex> removedVertices,
                                    Map<Id, VortexEdge> removedEdges) {
        // Remove related edges of each vertex
        for (VortexVertex v : removedVertices.values()) {
            if (!v.schemaLabel().existsLinkLabel()) {
                continue;
            }
            // Query all edges of the vertex and remove them
            Query query = constructEdgesQuery(v.id(), Directions.BOTH);
            Iterator<VortexEdge> vedges = this.queryEdgesFromBackend(query);
            try {
                while (vedges.hasNext()) {
                    this.checkTxEdgesCapacity();
                    VortexEdge edge = vedges.next();
                    // NOTE: will change the input parameter
                    removedEdges.put(edge.id(), edge);
                    // Commit first if enabled commit-part mode
                    if (this.commitPartOfAdjacentEdges > 0 &&
                        removedEdges.size() >= this.commitPartOfAdjacentEdges) {
                        this.commitPartOfEdgeDeletions(removedEdges);
                    }
                }
            } finally {
                CloseableIterator.closeIterator(vedges);
            }
        }

        // Remove vertices
        for (VortexVertex v : removedVertices.values()) {
            this.checkAggregateProperty(v);
            /*
             * If the backend stores vertex together with edges, it's edges
             * would be removed after removing vertex. Otherwise, if the
             * backend stores vertex which is separated from edges, it's
             * edges should be removed manually when removing vertex.
             */
            this.doRemove(this.serializer.writeVertex(v.prepareRemoved()));
            this.indexTx.updateVertexIndex(v, true);
            this.indexTx.updateLabelIndex(v, true);
        }

        // Remove edges
        this.prepareDeletions(removedEdges);
    }

    protected void prepareDeletions(Map<Id, VortexEdge> removedEdges) {
        // Remove edges
        for (VortexEdge e : removedEdges.values()) {
            this.checkAggregateProperty(e);
            // Update edge index
            this.indexTx.updateEdgeIndex(e, true);
            this.indexTx.updateLabelIndex(e, true);
            // Remove edge of OUT and IN
            e = e.prepareRemoved();
            this.doRemove(this.serializer.writeEdge(e));
            this.doRemove(this.serializer.writeEdge(e.switchOwner()));
        }
    }

    protected void prepareUpdates(Set<VortexProperty<?>> addedProps,
                                  Set<VortexProperty<?>> removedProps) {
        for (VortexProperty<?> p : removedProps) {
            this.checkAggregateProperty(p);
            if (p.element().type().isVertex()) {
                VortexVertexProperty<?> prop = (VortexVertexProperty<?>) p;
                if (this.store().features().supportsUpdateVertexProperty()) {
                    // Update vertex index without removed property
                    this.indexTx.updateVertexIndex(prop.element(), false);
                    // Eliminate the property(OUT and IN owner edge)
                    this.doEliminate(this.serializer.writeVertexProperty(prop));
                } else {
                    // Override vertex
                    this.addVertex(prop.element());
                }
            } else {
                assert p.element().type().isEdge();
                VortexEdgeProperty<?> prop = (VortexEdgeProperty<?>) p;
                if (this.store().features().supportsUpdateEdgeProperty()) {
                    // Update edge index without removed property
                    this.indexTx.updateEdgeIndex(prop.element(), false);
                    // Eliminate the property(OUT and IN owner edge)
                    this.doEliminate(this.serializer.writeEdgeProperty(prop));
                    this.doEliminate(this.serializer.writeEdgeProperty(
                                     prop.switchEdgeOwner()));
                } else {
                    // Override edge(it will be in addedEdges & updatedEdges)
                    this.addEdge(prop.element());
                }
            }
        }
        for (VortexProperty<?> p : addedProps) {
            this.checkAggregateProperty(p);
            if (p.element().type().isVertex()) {
                VortexVertexProperty<?> prop = (VortexVertexProperty<?>) p;
                if (this.store().features().supportsUpdateVertexProperty()) {
                    // Update vertex index with new added property
                    this.indexTx.updateVertexIndex(prop.element(), false);
                    // Append new property(OUT and IN owner edge)
                    this.doAppend(this.serializer.writeVertexProperty(prop));
                } else {
                    // Override vertex
                    this.addVertex(prop.element());
                }
            } else {
                assert p.element().type().isEdge();
                VortexEdgeProperty<?> prop = (VortexEdgeProperty<?>) p;
                if (this.store().features().supportsUpdateEdgeProperty()) {
                    // Update edge index with new added property
                    this.indexTx.updateEdgeIndex(prop.element(), false);
                    // Append new property(OUT and IN owner edge)
                    this.doAppend(this.serializer.writeEdgeProperty(prop));
                    this.doAppend(this.serializer.writeEdgeProperty(
                                  prop.switchEdgeOwner()));
                } else {
                    // Override edge (it will be in addedEdges & updatedEdges)
                    this.addEdge(prop.element());
                }
            }
        }
    }

    private void commitPartOfEdgeDeletions(Map<Id, VortexEdge> removedEdges) {
        assert this.commitPartOfAdjacentEdges > 0;

        this.prepareDeletions(removedEdges);

        BackendMutation mutation = this.mutation();
        BackendMutation idxMutation = this.indexTransaction().mutation();

        try {
            this.commitMutation2Backend(mutation, idxMutation);
        } catch (Throwable e) {
            this.rollbackBackend();
        } finally {
            mutation.clear();
            idxMutation.clear();
        }

        removedEdges.clear();
    }

    @Override
    public void commit() throws BackendException {
        try {
            super.commit();
        } finally {
            this.locksTable.unlock();
        }
    }

    @Override
    public void rollback() throws BackendException {
        // Rollback properties changes
        for (VortexProperty<?> prop : this.updatedOldestProps) {
            prop.element().setProperty(prop);
        }
        try {
            super.rollback();
        } finally {
            this.locksTable.unlock();
        }
    }

    @Override
    public QueryResults<BackendEntry> query(Query query) {
        if (!(query instanceof ConditionQuery)) {
            LOG.debug("Query{final:{}}", query);
            return super.query(query);
        }

        QueryList<BackendEntry> queries = this.optimizeQueries(query,
                                                               super::query);
        LOG.debug("{}", queries);
        return queries.empty() ? QueryResults.empty() :
                                 queries.fetch(this.pageSize);
    }

    @Override
    public Number queryNumber(Query query) {
        E.checkArgument(!this.hasUpdate(),
                        "It's not allowed to query number when " +
                        "there are uncommitted records.");

        if (!(query instanceof ConditionQuery)) {
            return super.queryNumber(query);
        }

        Aggregate aggregate = query.aggregateNotNull();

        QueryList<Number> queries = this.optimizeQueries(query, q -> {
            boolean indexQuery = q.getClass() == IdQuery.class;
            OptimizedType optimized = ((ConditionQuery) query).optimized();
            Number result;
            if (!indexQuery) {
                result = super.queryNumber(q);
            } else {
                E.checkArgument(aggregate.func() == AggregateFunc.COUNT,
                                "The %s operator on index is not supported now",
                                aggregate.func().string());
                if (this.optimizeAggrByIndex &&
                    optimized == OptimizedType.INDEX) {
                    // The ids size means results count (assume no left index)
                    result = q.idsSize();
                } else {
                    assert optimized == OptimizedType.INDEX_FILTER ||
                           optimized == OptimizedType.INDEX;
                    assert q.resultType().isVertex() || q.resultType().isEdge();
                    result = IteratorUtils.count(q.resultType().isVertex() ?
                                                 this.queryVertices(q) :
                                                 this.queryEdges(q));
                }
            }
            return new QueryResults<>(IteratorUtils.of(result), q);
        });

        QueryResults<Number> results = queries.empty() ?
                                       QueryResults.empty() :
                                       queries.fetch(this.pageSize);
        return aggregate.reduce(results.iterator());
    }

    @Watched(prefix = "graph")
    public VortexVertex addVertex(Object... keyValues) {
        return this.addVertex(this.constructVertex(true, keyValues));
    }

    @Watched("graph.addVertex-instance")
    public VortexVertex addVertex(VortexVertex vertex) {
        this.checkOwnerThread();
        assert !vertex.removed();

        // Override vertices in local `removedVertices`
        this.removedVertices.remove(vertex.id());
        try {
            this.locksTable.lockReads(LockUtil.VERTEX_LABEL_DELETE,
                                      vertex.schemaLabel().id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE,
                                      vertex.schemaLabel().indexLabels());
            // Ensure vertex label still exists from vertex-construct to lock
            this.graph().vertexLabel(vertex.schemaLabel().id());
            /*
             * No need to lock VERTEX_LABEL_ADD_UPDATE, because vertex label
             * update only can add nullable properties and user data, which is
             * unconcerned with add vertex
             */
            this.beforeWrite();
            this.addedVertices.put(vertex.id(), vertex);
            this.afterWrite();
        } catch (Throwable e){
            this.locksTable.unlock();
            throw e;
        }
        return vertex;
    }

    @Watched(prefix = "graph")
    public VortexVertex constructVertex(boolean verifyVL, Object... keyValues) {
        VortexElement.ElementKeys elemKeys = VortexElement.classifyKeys(keyValues);
        if (possibleOlapVertex(elemKeys)) {
            Id id = VortexVertex.getIdValue(elemKeys.id());
            VortexVertex vertex = VortexVertex.create(this, id,
                                                  VertexLabel.OLAP_VL);
            ElementHelper.attachProperties(vertex, keyValues);
            Iterator<VortexProperty<?>> iterator = vertex.getProperties()
                                                       .iterator();
            assert iterator.hasNext();
            if (iterator.next().propertyKey().olap()) {
                return vertex;
            }
        }
        VertexLabel vertexLabel = this.checkVertexLabel(elemKeys.label(),
                                                        verifyVL);
        Id id = VortexVertex.getIdValue(elemKeys.id());
        List<Id> keys = this.graph().mapPkName2Id(elemKeys.keys());

        // Check whether id match with id strategy
        this.checkId(id, keys, vertexLabel);

        // Create VortexVertex
        VortexVertex vertex = VortexVertex.create(this, null, vertexLabel);

        // Set properties
        ElementHelper.attachProperties(vertex, keyValues);

        // Assign vertex id
        if (this.params().mode().maintaining() &&
            vertexLabel.idStrategy() == IdStrategy.AUTOMATIC) {
            // Resume id for AUTOMATIC id strategy in restoring mode
            vertex.assignId(id, true);
        } else {
            vertex.assignId(id);
        }

        return vertex;
    }

    private boolean possibleOlapVertex(VortexElement.ElementKeys elemKeys) {
        return elemKeys.id() != null && elemKeys.label() == null &&
               elemKeys.keys().size() == 1;
    }

    @Watched(prefix = "graph")
    public void removeVertex(VortexVertex vertex) {
        this.checkOwnerThread();

        this.beforeWrite();

        // Override vertices in local `addedVertices`
        this.addedVertices.remove(vertex.id());

        // Collect the removed vertex
        this.removedVertices.put(vertex.id(), vertex);

        this.afterWrite();
    }

    public Iterator<Vertex> queryAdjacentVertices(Iterator<Edge> edges) {
        if (this.lazyLoadAdjacentVertex) {
            return new MapperIterator<>(edges, edge -> {
                return ((VortexEdge) edge).otherVertex();
            });
        }

        return new BatchMapperIterator<>(this.batchSize, edges, batchEdges -> {
            List<Id> vertexIds = new ArrayList<>(batchEdges.size());
            for (Edge edge : batchEdges) {
                vertexIds.add(((VortexEdge) edge).otherVertex().id());
            }
            assert vertexIds.size() > 0;
            return this.queryAdjacentVertices(vertexIds.toArray());
        });
    }

    public Iterator<Vertex> queryAdjacentVertices(Object... vertexIds) {
        return this.queryVerticesByIds(vertexIds, true,
                                       this.checkAdjacentVertexExist);
    }

    public Iterator<Vertex> queryVertices(Object... vertexIds) {
        return this.queryVerticesByIds(vertexIds, false, false);
    }

    public Vertex queryVertex(Object vertexId) {
        Iterator<Vertex> iter = this.queryVerticesByIds(new Object[]{vertexId},
                                                        false, true);
        Vertex vertex = QueryResults.one(iter);
        if (vertex == null) {
            throw new NotFoundException("Vertex '%s' does not exist", vertexId);
        }
        return vertex;
    }

    protected Iterator<Vertex> queryVerticesByIds(Object[] vertexIds,
                                                  boolean adjacentVertex,
                                                  boolean checkMustExist) {
        Query.checkForceCapacity(vertexIds.length);

        // NOTE: allowed duplicated vertices if query by duplicated ids
        List<Id> ids = InsertionOrderUtil.newList();
        Map<Id, VortexVertex> vertices = new HashMap<>(vertexIds.length);

        IdQuery query = new IdQuery(VortexType.VERTEX);
        for (Object vertexId : vertexIds) {
            VortexVertex vertex;
            Id id = VortexVertex.getIdValue(vertexId);
            if (id == null || this.removedVertices.containsKey(id)) {
                // The record has been deleted
                continue;
            } else if ((vertex = this.addedVertices.get(id)) != null ||
                       (vertex = this.updatedVertices.get(id)) != null) {
                if (vertex.expired()) {
                    continue;
                }
                // Found from local tx
                vertices.put(vertex.id(), vertex);
            } else {
                // Prepare to query from backend store
                query.query(id);
            }
            ids.add(id);
        }

        if (!query.empty()) {
            // Query from backend store
            query.mustSortByInput(false);
            Iterator<VortexVertex> it = this.queryVerticesFromBackend(query);
            QueryResults.fillMap(it, vertices);
        }

        return new MapperIterator<>(ids.iterator(), id -> {
            VortexVertex vertex = vertices.get(id);
            if (vertex == null) {
                if (checkMustExist) {
                    throw new NotFoundException(
                              "Vertex '%s' does not exist", id);
                } else if (adjacentVertex) {
                    assert !checkMustExist;
                    // Return undefined if adjacentVertex but !checkMustExist
                    vertex = VortexVertex.undefined(this.graph(), id);
                } else {
                    // Return null
                    assert vertex == null;
                }
            }
            return vertex;
        });
    }

    public Iterator<Vertex> queryVertices() {
        Query q = new Query(VortexType.VERTEX);
        return this.queryVertices(q);
    }

    public Iterator<Vertex> queryVertices(Query query) {
        E.checkArgument(this.removedVertices.isEmpty() || query.noLimit(),
                        "It's not allowed to query with limit when " +
                        "there are uncommitted delete records.");

        query.resetActualOffset();

        Iterator<VortexVertex> results = this.queryVerticesFromBackend(query);
        results = this.filterUnmatchedRecords(results, query);

        @SuppressWarnings("unchecked")
        Iterator<Vertex> r = (Iterator<Vertex>) joinTxVertices(query, results);
        return this.skipOffsetOrStopLimit(r, query);
    }

    protected Iterator<VortexVertex> queryVerticesFromBackend(Query query) {
        assert query.resultType().isVertex();

        QueryResults<BackendEntry> results = this.query(query);
        Iterator<BackendEntry> entries = results.iterator();

        Iterator<VortexVertex> vertices = new MapperIterator<>(entries,
                                                             this::parseEntry);
        vertices = this.filterExpiredResultFromFromBackend(query, vertices);

        if (!this.store().features().supportsQuerySortByInputIds()) {
            // There is no id in BackendEntry, so sort after deserialization
            vertices = results.keepInputOrderIfNeeded(vertices);
        }
        return vertices;
    }

    @Watched(prefix = "graph")
    public VortexEdge addEdge(VortexEdge edge) {
        this.checkOwnerThread();
        assert !edge.removed();

        // Override edges in local `removedEdges`
        this.removedEdges.remove(edge.id());
        try {
            this.locksTable.lockReads(LockUtil.EDGE_LABEL_DELETE,
                                      edge.schemaLabel().id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE,
                                      edge.schemaLabel().indexLabels());
            // Ensure edge label still exists from edge-construct to lock
            this.graph().edgeLabel(edge.schemaLabel().id());
            /*
             * No need to lock EDGE_LABEL_ADD_UPDATE, because edge label
             * update only can add nullable properties and user data, which is
             * unconcerned with add edge
             */
            this.beforeWrite();
            this.addedEdges.put(edge.id(), edge);
            this.afterWrite();
        } catch (Throwable e) {
            this.locksTable.unlock();
            throw e;
        }
        return edge;
    }

    @Watched(prefix = "graph")
    public void removeEdge(VortexEdge edge) {
        this.checkOwnerThread();

        this.beforeWrite();

        // Override edges in local `addedEdges`
        this.addedEdges.remove(edge.id());

        // Collect the removed edge
        this.removedEdges.put(edge.id(), edge);

        this.afterWrite();
    }

    public Iterator<Edge> queryEdgesByVertex(Id id) {
        return this.queryEdges(constructEdgesQuery(id, Directions.BOTH));
    }

    public Iterator<Edge> queryEdges(Object... edgeIds) {
        return this.queryEdgesByIds(edgeIds, false);
    }

    public Edge queryEdge(Object edgeId) {
        Iterator<Edge> iter = this.queryEdgesByIds(new Object[]{edgeId}, true);
        Edge edge = QueryResults.one(iter);
        if (edge == null) {
            throw new NotFoundException("Edge '%s' does not exist", edgeId);
        }
        return edge;
    }

    protected Iterator<Edge> queryEdgesByIds(Object[] edgeIds,
                                             boolean verifyId) {
        Query.checkForceCapacity(edgeIds.length);

        // NOTE: allowed duplicated edges if query by duplicated ids
        List<Id> ids = InsertionOrderUtil.newList();
        Map<Id, VortexEdge> edges = new HashMap<>(edgeIds.length);

        IdQuery query = new IdQuery(VortexType.EDGE);
        for (Object edgeId : edgeIds) {
            VortexEdge edge;
            EdgeId id = VortexEdge.getIdValue(edgeId, !verifyId);
            if (id == null) {
                continue;
            }
            if (id.direction() == Directions.IN) {
                id = id.switchDirection();
            }
            if (this.removedEdges.containsKey(id)) {
                // The record has been deleted
                continue;
            } else if ((edge = this.addedEdges.get(id)) != null ||
                       (edge = this.updatedEdges.get(id)) != null) {
                if (edge.expired()) {
                    continue;
                }
                // Found from local tx
                edges.put(edge.id(), edge);
            } else {
                // Prepare to query from backend store
                query.query(id);
            }
            ids.add(id);
        }

        if (!query.empty()) {
            // Query from backend store
            if (edges.isEmpty() && query.idsSize() == ids.size()) {
                /*
                 * Sort at the lower layer and return directly if there is no
                 * local vertex and duplicated id.
                 */
                Iterator<VortexEdge> it = this.queryEdgesFromBackend(query);
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Iterator<Edge> r = (Iterator) it;
                return r;
            }

            query.mustSortByInput(false);
            Iterator<VortexEdge> it = this.queryEdgesFromBackend(query);
            QueryResults.fillMap(it, edges);
        }

        return new MapperIterator<>(ids.iterator(), id -> {
            Edge edge = edges.get(id);
            return edge;
        });
    }

    public Iterator<Edge> queryEdges() {
        Query q = new Query(VortexType.EDGE);
        return this.queryEdges(q);
    }

    @Watched
    public Iterator<Edge> queryEdges(Query query) {
        E.checkArgument(this.removedEdges.isEmpty() || query.noLimit(),
                        "It's not allowed to query with limit when " +
                        "there are uncommitted delete records.");

        query.resetActualOffset();

        Iterator<VortexEdge> results = this.queryEdgesFromBackend(query);
        results = this.filterUnmatchedRecords(results, query);

        /*
         * Without repeated edges if not querying by BOTH all edges
         * TODO: any unconsidered case, maybe the query with OR condition?
         */
        boolean dedupEdge = false;
        if (dedupEdge) {
            Set<Id> returnedEdges = new HashSet<>();
            results = new FilterIterator<>(results, edge -> {
                // Filter duplicated edges (edge may be repeated query both)
                if (!returnedEdges.contains(edge.id())) {
                    /*
                     * NOTE: Maybe some edges are IN and others are OUT
                     * if querying edges both directions, perhaps it would look
                     * better if we convert all edges in results to OUT, but
                     * that would break the logic when querying IN edges.
                     */
                    returnedEdges.add(edge.id());
                    return true;
                } else {
                    LOG.debug("Result contains duplicated edge: {}", edge);
                    return false;
                }
            });
        }

        @SuppressWarnings("unchecked")
        Iterator<Edge> r = (Iterator<Edge>) joinTxEdges(query, results,
                                                        this.removedVertices);
        return this.skipOffsetOrStopLimit(r, query);
    }

    protected Iterator<VortexEdge> queryEdgesFromBackend(Query query) {
        assert query.resultType().isEdge();

        QueryResults<BackendEntry> results = this.query(query);
        Iterator<BackendEntry> entries = results.iterator();

        Iterator<VortexEdge> edges = new FlatMapperIterator<>(entries, entry -> {
            // Edges are in a vertex
            VortexVertex vertex = this.parseEntry(entry);
            if (vertex == null) {
                return null;
            }
            if (query.idsSize() == 1) {
                assert vertex.getEdges().size() == 1;
            }
            /*
             * Copy to avoid ConcurrentModificationException when removing edge
             * because VortexEdge.remove() will update edges in owner vertex
             */
            return new ListIterator<>(ImmutableList.copyOf(vertex.getEdges()));
        });

        edges = this.filterExpiredResultFromFromBackend(query, edges);

        if (!this.store().features().supportsQuerySortByInputIds()) {
            // There is no id in BackendEntry, so sort after deserialization
            edges = results.keepInputOrderIfNeeded(edges);
        }
        return edges;
    }

    @Watched(prefix = "graph")
    public <V> void addVertexProperty(VortexVertexProperty<V> prop) {
        // NOTE: this method can also be used to update property

        VortexVertex vertex = prop.element();
        E.checkState(vertex != null,
                     "No owner for updating property '%s'", prop.key());

        // Add property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.setProperty(prop);
            return;
        }
        // Check is updating property of added/removed vertex
        E.checkArgument(!this.addedVertices.containsKey(vertex.id()) ||
                        this.updatedVertices.containsKey(vertex.id()),
                        "Can't update property '%s' for adding-state vertex",
                        prop.key());
        E.checkArgument(!vertex.removed() &&
                        !this.removedVertices.containsKey(vertex.id()),
                        "Can't update property '%s' for removing-state vertex",
                        prop.key());
        // Check is updating primary key
        List<Id> primaryKeyIds = vertex.schemaLabel().primaryKeys();
        E.checkArgument(!primaryKeyIds.contains(prop.propertyKey().id()),
                        "Can't update primary key: '%s'", prop.key());

        // Do property update
        this.lockForUpdateProperty(vertex.schemaLabel(), prop, () -> {
            // Update old vertex to remove index (without new property)
            this.indexTx.updateVertexIndex(vertex, true);
            // Update(add) vertex property
            this.propertyUpdated(vertex, prop, vertex.setProperty(prop));
        });
    }

    @Watched(prefix = "graph")
    public <V> void removeVertexProperty(VortexVertexProperty<V> prop) {
        VortexVertex vertex = prop.element();
        PropertyKey propKey = prop.propertyKey();
        E.checkState(vertex != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed (compatible with tinkerpop)
        if (!vertex.hasProperty(propKey.id())) {
            // PropertyTest shouldAllowRemovalFromVertexWhenAlreadyRemoved()
            return;
        }
        // Check is removing primary key
        List<Id> primaryKeyIds = vertex.schemaLabel().primaryKeys();
        E.checkArgument(!primaryKeyIds.contains(propKey.id()),
                        "Can't remove primary key '%s'", prop.key());
        // Remove property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.removeProperty(propKey.id());
            return;
        }
        // Check is updating property of added/removed vertex
        E.checkArgument(!this.addedVertices.containsKey(vertex.id()) ||
                        this.updatedVertices.containsKey(vertex.id()),
                        "Can't remove property '%s' for adding-state vertex",
                        prop.key());
        E.checkArgument(!this.removedVertices.containsKey(vertex.id()),
                        "Can't remove property '%s' for removing-state vertex",
                        prop.key());

        // Do property update
        this.lockForUpdateProperty(vertex.schemaLabel(), prop, () -> {
            // Update old vertex to remove index (with the property)
            this.indexTx.updateVertexIndex(vertex, true);
            // Update(remove) vertex property
            VortexProperty<?> removed = vertex.removeProperty(propKey.id());
            this.propertyUpdated(vertex, null, removed);
        });
    }

    @Watched(prefix = "graph")
    public <V> void addEdgeProperty(VortexEdgeProperty<V> prop) {
        // NOTE: this method can also be used to update property

        VortexEdge edge = prop.element();
        E.checkState(edge != null,
                     "No owner for updating property '%s'", prop.key());

        // Add property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.setProperty(prop);
            return;
        }
        // Check is updating property of added/removed edge
        E.checkArgument(!this.addedEdges.containsKey(edge.id()) ||
                        this.updatedEdges.containsKey(edge.id()),
                        "Can't update property '%s' for adding-state edge",
                        prop.key());
        E.checkArgument(!edge.removed() &&
                        !this.removedEdges.containsKey(edge.id()),
                        "Can't update property '%s' for removing-state edge",
                        prop.key());
        // Check is updating sort key
        List<Id> sortKeys = edge.schemaLabel().sortKeys();
        E.checkArgument(!sortKeys.contains(prop.propertyKey().id()),
                        "Can't update sort key '%s'", prop.key());

        // Do property update
        this.lockForUpdateProperty(edge.schemaLabel(), prop, () -> {
            // Update old edge to remove index (without new property)
            this.indexTx.updateEdgeIndex(edge, true);
            // Update(add) edge property
            this.propertyUpdated(edge, prop, edge.setProperty(prop));
        });
    }

    @Watched(prefix = "graph")
    public <V> void removeEdgeProperty(VortexEdgeProperty<V> prop) {
        VortexEdge edge = prop.element();
        PropertyKey propKey = prop.propertyKey();
        E.checkState(edge != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed
        if (!edge.hasProperty(propKey.id())) {
            return;
        }
        // Check is removing sort key
        List<Id> sortKeyIds = edge.schemaLabel().sortKeys();
        E.checkArgument(!sortKeyIds.contains(prop.propertyKey().id()),
                        "Can't remove sort key '%s'", prop.key());
        // Remove property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.removeProperty(propKey.id());
            return;
        }
        // Check is updating property of added/removed edge
        E.checkArgument(!this.addedEdges.containsKey(edge.id()) ||
                        this.updatedEdges.containsKey(edge.id()),
                        "Can't remove property '%s' for adding-state edge",
                        prop.key());
        E.checkArgument(!this.removedEdges.containsKey(edge.id()),
                        "Can't remove property '%s' for removing-state edge",
                        prop.key());

        // Do property update
        this.lockForUpdateProperty(edge.schemaLabel(), prop, () -> {
            // Update old edge to remove index (with the property)
            this.indexTx.updateEdgeIndex(edge, true);
            // Update(remove) edge property
            this.propertyUpdated(edge, null,
                                 edge.removeProperty(propKey.id()));
        });
    }

    /**
     * Construct one edge condition query based on source vertex, direction and
     * edge labels
     * @param sourceVertex source vertex of edge
     * @param direction only be "IN", "OUT" or "BOTH"
     * @param edgeLabels edge labels of queried edges
     * @return constructed condition query
     */
    @Watched
    public static ConditionQuery constructEdgesQuery(Id sourceVertex,
                                                     Directions direction,
                                                     Id... edgeLabels) {
        E.checkState(sourceVertex != null,
                     "The edge query must contain source vertex");
        E.checkState(direction != null,
                     "The edge query must contain direction");

        ConditionQuery query = new ConditionQuery(VortexType.EDGE);

        // Edge source vertex
        query.eq(VortexKeys.OWNER_VERTEX, sourceVertex);

        // Edge direction
        if (direction == Directions.BOTH) {
            query.query(Condition.or(
                        Condition.eq(VortexKeys.DIRECTION, Directions.OUT),
                        Condition.eq(VortexKeys.DIRECTION, Directions.IN)));
        } else {
            assert direction == Directions.OUT || direction == Directions.IN;
            query.eq(VortexKeys.DIRECTION, direction);
        }

        // Edge labels
        if (edgeLabels.length == 1) {
            query.eq(VortexKeys.LABEL, edgeLabels[0]);
        } else if (edgeLabels.length > 1) {
            query.query(Condition.in(VortexKeys.LABEL,
                                     Arrays.asList(edgeLabels)));
        } else {
            assert edgeLabels.length == 0;
        }

        return query;
    }

    public static boolean matchFullEdgeSortKeys(ConditionQuery query,
                                                Vortex graph) {
        // All queryKeys in sortKeys
        return matchEdgeSortKeys(query, true, graph);
    }

    public static boolean matchPartialEdgeSortKeys(ConditionQuery query,
                                                   Vortex graph) {
        // Partial queryKeys in sortKeys
        return matchEdgeSortKeys(query, false, graph);
    }

    private static boolean matchEdgeSortKeys(ConditionQuery query,
                                             boolean matchAll,
                                             Vortex graph) {
        assert query.resultType().isEdge();
        Id label = query.condition(VortexKeys.LABEL);
        if (label == null) {
            return false;
        }
        List<Id> sortKeys = graph.edgeLabel(label).sortKeys();
        if (sortKeys.isEmpty()) {
            return false;
        }
        Set<Id> queryKeys = query.userpropKeys();
        for (int i = sortKeys.size(); i > 0; i--) {
            List<Id> subFields = sortKeys.subList(0, i);
            if (queryKeys.containsAll(subFields)) {
                if (queryKeys.size() == subFields.size() || !matchAll) {
                    /*
                     * Return true if:
                     * matchAll=true and all queryKeys are in sortKeys
                     *  or
                     * partial queryKeys are in sortKeys
                     */
                    return true;
                }
            }
        }
        return false;
    }

    private static void verifyVerticesConditionQuery(ConditionQuery query) {
        assert query.resultType().isVertex();

        int total = query.conditionsSize();
        if (total == 1) {
            /*
             * Supported query:
             *  1.query just by vertex label
             *  2.query just by PROPERTIES (like containsKey,containsValue)
             *  3.query with scan
             */
            if (query.containsCondition(VortexKeys.LABEL) ||
                query.containsCondition(VortexKeys.PROPERTIES) ||
                query.containsScanRelation()) {
                return;
            }
        }

        int matched = 0;
        if (query.containsCondition(VortexKeys.PROPERTIES)) {
            matched++;
            if (query.containsCondition(VortexKeys.LABEL)) {
                matched++;
            }
        }

        if (matched != total) {
            throw new VortexException("Not supported querying vertices by %s",
                                    query.conditions());
        }
    }

    private static void verifyEdgesConditionQuery(ConditionQuery query) {
        assert query.resultType().isEdge();

        int total = query.conditionsSize();
        if (total == 1) {
            /*
             * Supported query:
             *  1.query just by edge label
             *  2.query just by PROPERTIES (like containsKey,containsValue)
             *  3.query with scan
             */
            if (query.containsCondition(VortexKeys.LABEL) ||
                query.containsCondition(VortexKeys.PROPERTIES) ||
                query.containsScanRelation()) {
                return;
            }
        }

        int matched = 0;
        for (VortexKeys key : EdgeId.KEYS) {
            Object value = query.condition(key);
            if (value == null) {
                break;
            }
            matched++;
        }
        int count = matched;

        if (query.containsCondition(VortexKeys.PROPERTIES)) {
            matched++;
            if (count < 3 && query.containsCondition(VortexKeys.LABEL)) {
                matched++;
            }
        }

        if (matched != total) {
            throw new VortexException(
                      "Not supported querying edges by %s, expect %s",
                      query.conditions(), EdgeId.KEYS[count]);
        }
    }

    private <R> QueryList<R> optimizeQueries(Query query,
                                             QueryResults.Fetcher<R> fetcher) {
        boolean supportIn = this.storeFeatures().supportsQueryWithInCondition();
        QueryList<R> queries = new QueryList<>(query, fetcher);
        for (ConditionQuery cq: ConditionQueryFlatten.flatten(
                                (ConditionQuery) query, supportIn)) {
            // Optimize by sysprop
            Query q = this.optimizeQuery(cq);
            /*
             * NOTE: There are two possibilities for this query:
             * 1.sysprop-query, which would not be empty.
             * 2.index-query result(ids after optimization), which may be empty.
             */
            if (q == null) {
                queries.add(this.indexQuery(cq), this.batchSize);
            } else if (!q.empty()) {
                queries.add(q);
            }
        }
        return queries;
    }

    private Query optimizeQuery(ConditionQuery query) {
        if (query.idsSize() > 0) {
            throw new VortexException(
                      "Not supported querying by id and conditions: %s", query);
        }

        Id label = (Id) query.condition(VortexKeys.LABEL);

        // Optimize vertex query
        if (label != null && query.resultType().isVertex()) {
            VertexLabel vertexLabel = this.graph().vertexLabel(label);
            if (vertexLabel.idStrategy() == IdStrategy.PRIMARY_KEY) {
                List<Id> keys = vertexLabel.primaryKeys();
                E.checkState(!keys.isEmpty(),
                             "The primary keys can't be empty when using " +
                             "'%s' id strategy for vertex label '%s'",
                             IdStrategy.PRIMARY_KEY, vertexLabel.name());
                if (query.matchUserpropKeys(keys)) {
                    // Query vertex by label + primary-values
                    query.optimized(OptimizedType.PRIMARY_KEY);
                    String primaryValues = query.userpropValuesString(keys);
                    LOG.debug("Query vertices by primaryKeys: {}", query);
                    // Convert {vertex-label + primary-key} to vertex-id
                    Id id = SplicingIdGenerator.splicing(label.asString(),
                                                         primaryValues);
                    /*
                     * Just query by primary-key(id), ignore other userprop(if
                     * exists) that it will be filtered by queryVertices(Query)
                     */
                    return new IdQuery(query, id);
                }
            }
        }

        // Optimize edge query
        if (query.resultType().isEdge() && label != null &&
            query.condition(VortexKeys.OWNER_VERTEX) != null &&
            query.condition(VortexKeys.DIRECTION) != null &&
            matchEdgeSortKeys(query, false, this.graph())) {
            // Query edge by sourceVertex + direction + label + sort-values
            query.optimized(OptimizedType.SORT_KEYS);
            query = query.copy();
            // Serialize sort-values
            List<Id> keys = this.graph().edgeLabel(label).sortKeys();
            List<Condition> conditions =
                            GraphIndexTransaction.constructShardConditions(
                            query, keys, VortexKeys.SORT_VALUES);
            query.query(conditions);
            /*
             * Reset all userprop since transfered to sort-keys, ignore other
             * userprop(if exists) that it will be filtered by queryEdges(Query)
             */
            query.resetUserpropConditions();

            LOG.debug("Query edges by sortKeys: {}", query);
            return query;
        }

        /*
         * Query only by sysprops, like: by vertex label, by edge label.
         * NOTE: we assume sysprops would be indexed by backend store
         * but we don't support query edges only by direction/target-vertex.
         */
        if (query.allSysprop()) {
            if (query.resultType().isVertex()) {
                verifyVerticesConditionQuery(query);
            } else if (query.resultType().isEdge()) {
                verifyEdgesConditionQuery(query);
            }
            /*
             * Just support:
             *  1.not query by label
             *  2.or query by label and store supports this feature
             */
            boolean byLabel = (label != null && query.conditionsSize() == 1);
            if (!byLabel || this.store().features().supportsQueryByLabel()) {
                return query;
            }
        }

        return null;
    }

    private IdHolderList indexQuery(ConditionQuery query) {
        /*
         * Optimize by index-query
         * It will return a list of id (maybe empty) if success,
         * or throw exception if there is no any index for query properties.
         */
        this.beforeRead();
        try {
            return this.indexTx.queryIndex(query);
        } finally {
            this.afterRead();
        }
    }

    private VertexLabel checkVertexLabel(Object label, boolean verifyLabel) {
        VortexVertexFeatures features = graph().features().vertex();

        // Check Vertex label
        if (label == null && features.supportsDefaultLabel()) {
            label = features.defaultLabel();
        }

        if (label == null) {
            throw Element.Exceptions.labelCanNotBeNull();
        }

        E.checkArgument(label instanceof String || label instanceof VertexLabel,
                        "Expect a string or a VertexLabel object " +
                        "as the vertex label argument, but got: '%s'", label);
        // The label must be an instance of String or VertexLabel
        if (label instanceof String) {
            if (verifyLabel) {
                ElementHelper.validateLabel((String) label);
            }
            label = graph().vertexLabel((String) label);
        }

        assert (label instanceof VertexLabel);
        return (VertexLabel) label;
    }

    private void checkId(Id id, List<Id> keys, VertexLabel vertexLabel) {
        // Check whether id match with id strategy
        IdStrategy strategy = vertexLabel.idStrategy();
        switch (strategy) {
            case PRIMARY_KEY:
                E.checkArgument(id == null,
                                "Can't customize vertex id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                // Check whether primaryKey exists
                List<Id> primaryKeys = vertexLabel.primaryKeys();
                E.checkArgument(keys.containsAll(primaryKeys),
                                "The primary keys: %s of vertex label '%s' " +
                                "must be set when using '%s' id strategy",
                                this.graph().mapPkId2Name(primaryKeys),
                                vertexLabel.name(), strategy);
                break;
            case AUTOMATIC:
                if (this.params().mode().maintaining()) {
                    E.checkArgument(id != null && id.number(),
                                    "Must customize vertex number id when " +
                                    "id strategy is '%s' for vertex label " +
                                    "'%s' in restoring mode",
                                    strategy, vertexLabel.name());
                } else {
                    E.checkArgument(id == null,
                                    "Can't customize vertex id when " +
                                    "id strategy is '%s' for vertex label '%s'",
                                    strategy, vertexLabel.name());
                }
                break;
            case CUSTOMIZE_STRING:
            case CUSTOMIZE_UUID:
                E.checkArgument(id != null && !id.number(),
                                "Must customize vertex string id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                break;
            case CUSTOMIZE_NUMBER:
                E.checkArgument(id != null && id.number(),
                                "Must customize vertex number id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                break;
            default:
                throw new AssertionError("Unknown id strategy: " + strategy);
        }
    }

    private void checkAggregateProperty(VortexElement element) {
        E.checkArgument(element.getAggregateProperties().isEmpty() ||
                        this.store().features().supportsAggregateProperty(),
                        "The %s store does not support aggregate property",
                        this.store().provider().type());
    }

    private void checkAggregateProperty(VortexProperty<?> property) {
        E.checkArgument(!property.isAggregateType() ||
                        this.store().features().supportsAggregateProperty(),
                        "The %s store does not support aggregate property",
                        this.store().provider().type());
    }

    private void checkNonnullProperty(VortexVertex vertex) {
        Set<Id> keys = vertex.getPropertyKeys();
        VertexLabel vertexLabel = vertex.schemaLabel();
        // Check whether passed all non-null property
        @SuppressWarnings("unchecked")
        Collection<Id> nonNullKeys = CollectionUtils.subtract(
                                     vertexLabel.properties(),
                                     vertexLabel.nullableKeys());
        if (!keys.containsAll(nonNullKeys)) {
            @SuppressWarnings("unchecked")
            Collection<Id> missed = CollectionUtils.subtract(nonNullKeys, keys);
            Vortex graph = this.graph();
            E.checkArgument(false, "All non-null property keys %s of " +
                            "vertex label '%s' must be setted, missed keys %s",
                            graph.mapPkId2Name(nonNullKeys), vertexLabel.name(),
                            graph.mapPkId2Name(missed));
        }
    }

    private void checkVertexExistIfCustomizedId(Map<Id, VortexVertex> vertices) {
        Set<Id> ids = new HashSet<>();
        for (VortexVertex vertex : vertices.values()) {
            VertexLabel vl = vertex.schemaLabel();
            if (!vl.hidden() && vl.idStrategy().isCustomized()) {
                ids.add(vertex.id());
            }
        }
        if (ids.isEmpty()) {
            return;
        }
        IdQuery idQuery = new IdQuery(VortexType.VERTEX, ids);
        Iterator<VortexVertex> results = this.queryVerticesFromBackend(idQuery);
        try {
            if (!results.hasNext()) {
                return;
            }
            VortexVertex existedVertex = results.next();
            VortexVertex newVertex = vertices.get(existedVertex.id());
            if (!existedVertex.label().equals(newVertex.label())) {
                throw new VortexException(
                          "The newly added vertex with id:'%s' label:'%s' " +
                          "is not allowed to insert, because already exist " +
                          "a vertex with same id and different label:'%s'",
                          newVertex.id(), newVertex.label(),
                          existedVertex.label());
            }
        } finally {
            CloseableIterator.closeIterator(results);
        }
    }

    private void lockForUpdateProperty(SchemaLabel schemaLabel,
                                       VortexProperty<?> prop,
                                       Runnable callback) {
        this.checkOwnerThread();

        Id pkey = prop.propertyKey().id();
        Set<Id> indexIds = new HashSet<>();
        for (Id il : schemaLabel.indexLabels()) {
            if (graph().indexLabel(il).indexFields().contains(pkey)) {
                indexIds.add(il);
            }
        }
        String group = schemaLabel.type() == VortexType.VERTEX_LABEL ?
                       LockUtil.VERTEX_LABEL_DELETE :
                       LockUtil.EDGE_LABEL_DELETE;
        try {
            this.locksTable.lockReads(group, schemaLabel.id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE, indexIds);
            // Ensure schema label still exists
            if (schemaLabel.type() == VortexType.VERTEX_LABEL) {
                this.graph().vertexLabel(schemaLabel.id());
            } else {
                assert schemaLabel.type() == VortexType.EDGE_LABEL;
                this.graph().edgeLabel(schemaLabel.id());
            }
            /*
             * No need to lock INDEX_LABEL_ADD_UPDATE, because index label
             * update only can add  user data, which is unconcerned with
             * update property
             */
            this.beforeWrite();
            callback.run();
            this.afterWrite();
        } catch (Throwable e) {
            this.locksTable.unlock();
            throw e;
        }
    }

    private void removeLeftIndexIfNeeded(Map<Id, VortexVertex> vertices) {
        Set<Id> ids = vertices.keySet();
        if (ids.isEmpty()) {
            return;
        }
        IdQuery idQuery = new IdQuery(VortexType.VERTEX, ids);
        Iterator<VortexVertex> results = this.queryVerticesFromBackend(idQuery);
        try {
            while (results.hasNext()) {
                VortexVertex existedVertex = results.next();
                this.indexTx.updateVertexIndex(existedVertex, true);
            }
        } finally {
            CloseableIterator.closeIterator(results);
        }
    }

    private <T extends VortexElement> Iterator<T> filterUnmatchedRecords(
                                                Iterator<T> results,
                                                Query query) {
        // Filter unused or incorrect records
        return new FilterIterator<T>(results, elem -> {
            // TODO: Left vertex/edge should to be auto removed via async task
            if (elem.schemaLabel().undefined()) {
                LOG.warn("Left record is found: id={}, label={}, properties={}",
                         elem.id(), elem.schemaLabel().id(),
                         elem.getPropertiesMap());
            }
            // Filter hidden results
            if (!query.showHidden() && Graph.Hidden.isHidden(elem.label())) {
                return false;
            }
            // Filter vertices/edges of deleting label
            if (elem.schemaLabel().status().deleting() &&
                !query.showDeleting()) {
                return false;
            }
            // Process results that query from left index or primary-key
            if (query.resultType().isVertex() == elem.type().isVertex() &&
                !rightResultFromIndexQuery(query, elem)) {
                // Only index query will come here
                return false;
            }
            return true;
        });
    }

    private boolean rightResultFromIndexQuery(Query query, VortexElement elem) {
        /*
         * If query is ConditionQuery or query.originQuery() is ConditionQuery
         * means it's index query
         */
        if (!(query instanceof ConditionQuery)) {
            if (query.originQuery() instanceof ConditionQuery) {
                query = query.originQuery();
            } else {
                return true;
            }
        }

        ConditionQuery cq = (ConditionQuery) query;
        if (cq.optimized() == OptimizedType.NONE || cq.test(elem)) {
            if (cq.existLeftIndex(elem.id())) {
                /*
                 * Both have correct and left index, wo should return true
                 * but also needs to cleaned up left index
                 */
                this.indexTx.asyncRemoveIndexLeft(cq, elem);
            }

            /* Return true if:
             * 1.not query by index or by primary-key/sort-key
             *   (cq.optimized() == 0 means query just by sysprop)
             * 2.the result match all conditions
             */
            return true;
        }

        if (cq.optimized() == OptimizedType.INDEX) {
            this.indexTx.asyncRemoveIndexLeft(cq, elem);
        }
        return false;
    }

    private <T extends VortexElement> Iterator<T>
                                    filterExpiredResultFromFromBackend(
                                    Query query, Iterator<T> results) {
        if (this.store().features().supportsTtl() || query.showExpired()) {
            return results;
        }
        // Filter expired vertices/edges with TTL
        return new FilterIterator<>(results, elem -> {
            if (elem.expired()) {
                DeleteExpiredJob.asyncDeleteExpiredObject(this.graph(), elem);
                return false;
            }
            return true;
        });
    }

    private <T> Iterator<T> skipOffsetOrStopLimit(Iterator<T> results,
                                                  Query query) {
        if (query.noLimitAndOffset()) {
            return results;
        }
        // Skip offset
        long offset = query.offset();
        if (offset > 0L && results.hasNext()) {
            /*
             * Must call results.hasNext() before query.actualOffset() due to
             * some backends will go offset and update query.actualOffset
             */
            long current = query.actualOffset();
            for (; current < offset && results.hasNext(); current++) {
                results.next();
                query.goOffset(1L);
            }
        }
        // Stop if reach limit
        return new LimitIterator<>(results, elem -> {
            long count = query.goOffset(1L);
            return query.reachLimit(count - 1L);
        });
    }

    private Iterator<?> joinTxVertices(Query query,
                                       Iterator<VortexVertex> vertices) {
        assert query.resultType().isVertex();
        BiFunction<Query, VortexVertex, VortexVertex> matchTxFunc = (q, v) -> {
            if (v.expired() && !q.showExpired()) {
                // Filter expired vertices with TTL
                return null;
            }
            // Filter vertices matched conditions
            return q.test(v) ? v : null;
        };
        vertices =  this.joinTxRecords(query, vertices, matchTxFunc,
                                       this.addedVertices, this.removedVertices,
                                       this.updatedVertices);
        return vertices;
    }

    private Iterator<?> joinTxEdges(Query query, Iterator<VortexEdge> edges,
                                    Map<Id, VortexVertex> removingVertices) {
        assert query.resultType().isEdge();
        BiFunction<Query, VortexEdge, VortexEdge> matchTxFunc = (q, e) -> {
            assert q.resultType() == VortexType.EDGE;
            if (e.expired() && !q.showExpired()) {
                // Filter expired edges with TTL
                return null;
            }
            // Filter edges matched conditions
            return q.test(e) ? e : q.test(e = e.switchOwner()) ? e : null;
        };
        edges = this.joinTxRecords(query, edges, matchTxFunc,
                                   this.addedEdges, this.removedEdges,
                                   this.updatedEdges);
        if (removingVertices.isEmpty()) {
            return edges;
        }
        // Filter edges that belong to deleted vertex
        return new FilterIterator<VortexEdge>(edges, edge -> {
            for (VortexVertex v : removingVertices.values()) {
                if (edge.belongToVertex(v)) {
                    return false;
                }
            }
            return true;
        });
    }

    private <V extends VortexElement> Iterator<V> joinTxRecords(
                                    Query query,
                                    Iterator<V> records,
                                    BiFunction<Query, V, V> matchFunc,
                                    Map<Id, V> addedTxRecords,
                                    Map<Id, V> removedTxRecords,
                                    Map<Id, V> updatedTxRecords) {
        this.checkOwnerThread();
        // Return the origin results if there is no change in tx
        if (addedTxRecords.isEmpty() &&
            removedTxRecords.isEmpty() &&
            updatedTxRecords.isEmpty()) {
            return records;
        }

        Set<V> txResults = InsertionOrderUtil.newSet();

        /*
         * Collect added/updated records
         * Records in memory have higher priority than query from backend store
         */
        for (V elem : addedTxRecords.values()) {
            if (query.reachLimit(txResults.size())) {
                break;
            }
            if ((elem = matchFunc.apply(query, elem)) != null) {
                txResults.add(elem);
            }
        }
        for (V elem : updatedTxRecords.values()) {
            if (query.reachLimit(txResults.size())) {
                break;
            }
            if ((elem = matchFunc.apply(query, elem)) != null) {
                txResults.add(elem);
            }
        }

        // Filter backend record if it's updated in memory
        Iterator<V> backendResults = new FilterIterator<>(records, elem -> {
            Id id = elem.id();
            return !addedTxRecords.containsKey(id) &&
                   !updatedTxRecords.containsKey(id) &&
                   !removedTxRecords.containsKey(id);
        });

        return new ExtendableIterator<V>(txResults.iterator(), backendResults);
    }

    private void checkTxVerticesCapacity() throws LimitExceedException {
        if (this.verticesInTxSize() >= this.verticesCapacity) {
            throw new LimitExceedException(
                      "Vertices size has reached tx capacity %d",
                      this.verticesCapacity);
        }
    }

    private void checkTxEdgesCapacity() throws LimitExceedException {
        if (this.edgesInTxSize() >= this.edgesCapacity) {
            throw new LimitExceedException(
                      "Edges size has reached tx capacity %d",
                      this.edgesCapacity);
        }
    }

    private void propertyUpdated(VortexElement element, VortexProperty<?> property,
                                 VortexProperty<?> oldProperty) {
        if (element.type().isVertex()) {
            this.updatedVertices.put(element.id(), (VortexVertex) element);
        } else {
            assert element.type().isEdge();
            this.updatedEdges.put(element.id(), (VortexEdge) element);
        }

        if (oldProperty != null) {
            this.updatedOldestProps.add(oldProperty);
        }
        if (property == null) {
            this.removedProps.add(oldProperty);
        } else {
            this.addedProps.remove(property);
            this.addedProps.add(property);
        }
    }

    private VortexVertex parseEntry(BackendEntry entry) {
        try {
            VortexVertex vertex = this.serializer.readVertex(graph(), entry);
            assert vertex != null;
            return vertex;
        } catch (ForbiddenException | SecurityException e) {
            /*
             * Can't ignore permission exception here, otherwise users will
             * be confused to treat as the record does not exist.
             */
            throw e;
        } catch (Throwable e) {
            LOG.error("Failed to parse entry: {}", entry, e);
            if (this.ignoreInvalidEntry) {
                return null;
            }
            throw e;
        }
    }

    /*
     * TODO: set these methods to protected
     */
    public void removeIndex(IndexLabel indexLabel) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.beforeWrite();
        this.indexTx.removeIndex(indexLabel);
        this.afterWrite();
    }

    public void updateIndex(Id ilId, VortexElement element, boolean removed) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.indexTx.updateIndex(ilId, element, removed);
    }

    public void removeIndex(VortexIndex index) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.beforeWrite();
        this.indexTx.doEliminate(this.serializer.writeIndex(index));
        this.afterWrite();
    }

    public void removeVertices(VertexLabel vertexLabel) {
        if (this.hasUpdate()) {
            throw new VortexException("There are still changes to commit");
        }

        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);
        // Commit data already in tx firstly
        this.commit();
        try {
            this.traverseVerticesByLabel(vertexLabel, vertex -> {
                this.removeVertex((VortexVertex) vertex);
                this.commitIfGtSize(COMMIT_BATCH);
            }, true);
            this.commit();
        } catch (Exception e) {
            LOG.error("Failed to remove vertices", e);
            throw new VortexException("Failed to remove vertices", e);
        } finally {
            this.autoCommit(autoCommit);
        }
    }

    public void removeEdges(EdgeLabel edgeLabel) {
        if (this.hasUpdate()) {
            throw new VortexException("There are still changes to commit");
        }

        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);
        // Commit data already in tx firstly
        this.commit();
        try {
            if (this.store().features().supportsDeleteEdgeByLabel()) {
                // TODO: Need to change to writeQuery!
                this.doRemove(this.serializer.writeId(VortexType.EDGE_OUT,
                                                      edgeLabel.id()));
                this.doRemove(this.serializer.writeId(VortexType.EDGE_IN,
                                                      edgeLabel.id()));
            } else {
                this.traverseEdgesByLabel(edgeLabel, edge -> {
                    this.removeEdge((VortexEdge) edge);
                    this.commitIfGtSize(COMMIT_BATCH);
                }, true);
            }
            this.commit();
        } catch (Exception e) {
            LOG.error("Failed to remove edges", e);
            throw new VortexException("Failed to remove edges", e);
        } finally {
            this.autoCommit(autoCommit);
        }
    }

    public void traverseVerticesByLabel(VertexLabel label,
                                        Consumer<Vertex> consumer,
                                        boolean deleting) {
        this.traverseByLabel(label, this::queryVertices, consumer, deleting);
    }

    public void traverseEdgesByLabel(EdgeLabel label, Consumer<Edge> consumer,
                                     boolean deleting) {
        this.traverseByLabel(label, this::queryEdges, consumer, deleting);
    }

    private <T> void traverseByLabel(SchemaLabel label,
                                     Function<Query, Iterator<T>> fetcher,
                                     Consumer<T> consumer, boolean deleting) {
        VortexType type = label.type() == VortexType.VERTEX_LABEL ?
                        VortexType.VERTEX : VortexType.EDGE;
        Query query = label.enableLabelIndex() ? new ConditionQuery(type) :
                                                 new Query(type);
        query.capacity(Query.NO_CAPACITY);
        query.limit(Query.NO_LIMIT);
        if (this.store().features().supportsQueryByPage()) {
            query.page(PageInfo.PAGE_NONE);
        }
        if (label.hidden()) {
            query.showHidden(true);
        }
        query.showDeleting(deleting);
        query.showExpired(deleting);

        if (label.enableLabelIndex()) {
            // Support label index, query by label index by paging
            ((ConditionQuery) query).eq(VortexKeys.LABEL, label.id());
            Iterator<T> iter = fetcher.apply(query);
            try {
                // Fetch by paging automatically
                while (iter.hasNext()) {
                    consumer.accept(iter.next());
                    /*
                     * Commit per batch to avoid too much data in single commit,
                     * especially for Cassandra backend
                     */
                    this.commitIfGtSize(GraphTransaction.COMMIT_BATCH);
                }
                // Commit changes if exists
                this.commit();
            } finally {
                CloseableIterator.closeIterator(iter);
            }
        } else {
            // Not support label index, query all and filter by label
            if (query.paging()) {
                query.limit(this.pageSize);
            }
            String page = null;
            do {
                Iterator<T> iter = fetcher.apply(query);
                try {
                    while (iter.hasNext()) {
                        T e = iter.next();
                        SchemaLabel elemLabel = ((VortexElement) e).schemaLabel();
                        if (label.equals(elemLabel)) {
                            consumer.accept(e);
                            /*
                             * Commit per batch to avoid too much data in single
                             * commit, especially for Cassandra backend
                             */
                            this.commitIfGtSize(GraphTransaction.COMMIT_BATCH);
                        }
                    }
                    // Commit changes of every page before next page query
                    this.commit();
                    if (query.paging()) {
                        page = PageInfo.pageState(iter).toString();
                        query.page(page);
                    }
                } finally {
                    CloseableIterator.closeIterator(iter);
                }
            } while (page != null);
        }
    }

    public void createOlapPk(Id pkId) {
        this.store().createOlapTable(pkId);
    }

    public void initAndRegisterOlapTable(Id pkId) {
        this.store().checkAndRegisterOlapTable(pkId);
    }

    public void clearOlapPk(Id pkId) {
        this.store().clearOlapTable(pkId);
    }

    public void removeOlapPk(Id pkId) {
        this.store().removeOlapTable(pkId);
    }
}
