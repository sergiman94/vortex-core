
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.Log;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;

import java.util.*;

public final class VortexStep<S, E extends Element>
             extends GraphStep<S, E> implements QueryHolder {

    private static final long serialVersionUID = -679873894532085972L;

    private static final Logger LOG = Log.logger(VortexStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();

    // Store limit/order-by
    private final Query queryInfo = new Query(VortexType.UNKNOWN);

    private Iterator<E> lastTimeResults = QueryResults.emptyIterator();

    public VortexStep(final GraphStep<S, E> originGraphStep) {
        super(originGraphStep.getTraversal(),
              originGraphStep.getReturnClass(),
              originGraphStep.isStartStep(),
              originGraphStep.getIds());

        originGraphStep.getLabels().forEach(this::addLabel);

        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        this.setIteratorSupplier(() -> {
            Iterator<E> results = queryVertex ? this.vertices() : this.edges();
            this.lastTimeResults = results;
            return results;
        });
    }

    protected long count() {
        if (this.returnsVertex()) {
            return this.verticesCount();
        } else {
            assert this.returnsEdge();
            return this.edgesCount();
        }
    }

    private long verticesCount() {
        if (!this.hasIds()) {
            Vortex graph = TraversalUtil.getGraph(this);
            Query query = this.makeQuery(graph, VortexType.VERTEX);
            return graph.queryNumber(query).longValue();
        }
        return IteratorUtils.count(this.vertices());
    }

    private long edgesCount() {
        if (!this.hasIds()) {
            Vortex graph = TraversalUtil.getGraph(this);
            Query query = this.makeQuery(graph, VortexType.EDGE);
            return graph.queryNumber(query).longValue();
        }
        return IteratorUtils.count(this.edges());
    }

    private Iterator<E> vertices() {
        LOG.debug("VortexStep.vertices(): {}", this);

        Vortex graph = TraversalUtil.getGraph(this);
        // g.V().hasId(EMPTY_LIST) will set ids to null
        if (this.ids == null) {
            return QueryResults.emptyIterator();
        }

        if (this.hasIds()) {
            return TraversalUtil.filterResult(this.hasContainers,
                                              graph.vertices(this.ids));
        }

        Query query = this.makeQuery(graph, VortexType.VERTEX);
        @SuppressWarnings("unchecked")
        Iterator<E> result = (Iterator<E>) graph.vertices(query);
        return result;
    }

    private Iterator<E> edges() {
        LOG.debug("VortexStep.edges(): {}", this);

        Vortex graph = TraversalUtil.getGraph(this);

        // g.E().hasId(EMPTY_LIST) will set ids to null
        if (this.ids == null) {
            return QueryResults.emptyIterator();
        }

        if (this.hasIds()) {
            return TraversalUtil.filterResult(this.hasContainers,
                                              graph.edges(this.ids));
        }

        Query query = this.makeQuery(graph, VortexType.EDGE);
        @SuppressWarnings("unchecked")
        Iterator<E> result = (Iterator<E>) graph.edges(query);
        return result;
    }

    private boolean hasIds() {
        return this.ids != null && this.ids.length > 0;
    }

    private Query makeQuery(Vortex graph, VortexType type) {
        Query query = null;
        if (this.hasContainers.isEmpty()) {
            // Query all
            query = new Query(type);
        } else {
            ConditionQuery q = new ConditionQuery(type);
            query = TraversalUtil.fillConditionQuery(q, this.hasContainers,
                                                     graph);
        }

        query = this.injectQueryInfo(query);
        return query;
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        }

        return this.ids.length == 0 ?
               StringFactory.stepString(this,
                                        this.returnClass.getSimpleName(),
                                        this.hasContainers) :
               StringFactory.stepString(this,
                                        this.returnClass.getSimpleName(),
                                        Arrays.toString(this.ids),
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
        return this.lastTimeResults;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^
               this.queryInfo.hashCode() ^
               this.hasContainers.hashCode();
    }
}
