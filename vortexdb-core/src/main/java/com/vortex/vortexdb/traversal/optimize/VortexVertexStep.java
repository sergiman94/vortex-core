
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.Log;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import java.util.*;

public class VortexVertexStep<E extends Element>
       extends VertexStep<E> implements QueryHolder {

    private static final long serialVersionUID = -7850636388424382454L;

    private static final Logger LOG = Log.logger(VortexVertexStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();

    // Store limit/order-by
    private final Query queryInfo = new Query(null);

    private Iterator<E> iterator = QueryResults.emptyIterator();

    public VortexVertexStep(final VertexStep<E> originVertexStep) {
        super(originVertexStep.getTraversal(),
              originVertexStep.getReturnClass(),
              originVertexStep.getDirection(),
              originVertexStep.getEdgeLabels());
        originVertexStep.getLabels().forEach(this::addLabel);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        if (queryVertex) {
            this.iterator = (Iterator<E>) this.vertices(traverser);
        } else {
            assert queryEdge;
            this.iterator = (Iterator<E>) this.edges(traverser);
        }
        return this.iterator;
    }

    private Iterator<Vertex> vertices(Traverser.Admin<Vertex> traverser) {
        Iterator<Edge> edges = this.edges(traverser);
        Iterator<Vertex> vertices = this.queryAdjacentVertices(edges);

        if (LOG.isDebugEnabled()) {
            Vertex vertex = traverser.get();
            LOG.debug("VortexVertexStep.vertices(): is there adjacent " +
                      "vertices of {}: {}, has={}",
                      vertex.id(), vertices.hasNext(), this.hasContainers);
        }

        return vertices;
    }

    private Iterator<Edge> edges(Traverser.Admin<Vertex> traverser) {
        Query query = this.constructEdgesQuery(traverser);
        return this.queryEdges(query);
    }

    protected Iterator<Vertex> queryAdjacentVertices(Iterator<Edge> edges) {
        Vortex graph = TraversalUtil.getGraph(this);
        Iterator<Vertex> vertices = graph.adjacentVertices(edges);

        if (!this.withVertexCondition()) {
            return vertices;
        }

        // TODO: query by vertex index to optimize
        return TraversalUtil.filterResult(this.hasContainers, vertices);
    }

    protected Iterator<Edge> queryEdges(Query query) {
        Vortex graph = TraversalUtil.getGraph(this);

        // Do query
        Iterator<Edge> edges = graph.edges(query);

        if (!this.withEdgeCondition()) {
            return edges;
        }

        // Do filter by edge conditions
        return TraversalUtil.filterResult(this.hasContainers, edges);
    }

    protected ConditionQuery constructEdgesQuery(
                             Traverser.Admin<Vertex> traverser) {
        Vortex graph = TraversalUtil.getGraph(this);

        // Query for edge with conditions(else conditions for vertex)
        boolean withEdgeCond = this.withEdgeCondition();
        boolean withVertexCond = this.withVertexCondition();

        Id vertex = (Id) traverser.get().id();
        Directions direction = Directions.convert(this.getDirection());
        Id[] edgeLabels = graph.mapElName2Id(this.getEdgeLabels());

        LOG.debug("VortexVertexStep.edges(): vertex={}, direction={}, " +
                  "edgeLabels={}, has={}",
                  vertex, direction, edgeLabels, this.hasContainers);

        ConditionQuery query = GraphTransaction.constructEdgesQuery(
                               vertex, direction, edgeLabels);
        // Query by sort-keys
        if (withEdgeCond && edgeLabels.length == 1) {
            TraversalUtil.fillConditionQuery(query, this.hasContainers, graph);
            if (!GraphTransaction.matchPartialEdgeSortKeys(query, graph)) {
                // Can't query by sysprop and by index (Vortex-749)
                query.resetUserpropConditions();
            } else if (GraphTransaction.matchFullEdgeSortKeys(query, graph)) {
                // All sysprop conditions are in sort-keys
                withEdgeCond = false;
            } else {
                // Partial sysprop conditions are in sort-keys
                assert query.userpropKeys().size() > 0;
            }
        }

        // Query by has(id)
        if (query.idsSize() > 0) {
            // Ignore conditions if query by edge id in has-containers
            // FIXME: should check that the edge id matches the `vertex`
            query.resetConditions();
            LOG.warn("It's not recommended to query by has(id)");
        }

        /*
         * Unset limit when needed to filter property after store query
         * like query: outE().has(k,v).limit(n)
         * NOTE: outE().limit(m).has(k,v).limit(n) will also be unset limit,
         * Can't unset limit if query by paging due to page position will be
         * exceeded when reaching the limit in tinkerpop layer
         */
        if (withEdgeCond || withVertexCond) {
            com.vortex.common.util.E.checkArgument(!this.queryInfo().paging(),
                                                     "Can't query by paging " +
                                                     "and filtering");
            this.queryInfo().limit(Query.NO_LIMIT);
        }

        query = this.injectQueryInfo(query);

        return query;
    }

    protected boolean withVertexCondition() {
        return this.returnsVertex() && !this.hasContainers.isEmpty();
    }

    protected boolean withEdgeCondition() {
        return this.returnsEdge() && !this.hasContainers.isEmpty();
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        }

        return StringFactory.stepString(
               this,
               getDirection(),
               Arrays.asList(getEdgeLabels()),
               getReturnClass().getSimpleName(),
               this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer has) {
        if (SYSPROP_PAGE.equals(has.getKey())) {
            this.setPage((String) has.getValue());
            return;
        }
        this.hasContainers.add(has);
    }

    @Override
    public Query queryInfo() {
        return this.queryInfo;
    }

    @Override
    public Iterator<?> lastTimeResults() {
        return this.iterator;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^
               this.queryInfo.hashCode() ^
               this.hasContainers.hashCode();
    }
}
