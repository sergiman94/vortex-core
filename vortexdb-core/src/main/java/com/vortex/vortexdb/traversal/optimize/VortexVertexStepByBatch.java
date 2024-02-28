
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.vortexdb.backend.query.BatchConditionQuery;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.common.iterator.BatchMapperIterator;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.iterator.BatchMapperIterator;
import com.vortex.vortexdb.backend.query.Query;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;
import java.util.List;

public class VortexVertexStepByBatch<E extends Element>
       extends VortexVertexStep<E> {

    private static final long serialVersionUID = -3609787815053052222L;

    private BatchMapperIterator<Traverser.Admin<Vertex>, E> batchIterator;
    private Traverser.Admin<Vertex> head;
    private Iterator<E> iterator;

    public VortexVertexStepByBatch(final VertexStep<E> originalVertexStep) {
        super(originalVertexStep);
        this.batchIterator = null;
        this.head = null;
        this.iterator = null;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        /* Override super.processNextStart() */
        if (this.batchIterator == null) {
            int batchSize = (int) Query.QUERY_BATCH;
            this.batchIterator = new BatchMapperIterator<>(
                                 batchSize, this.starts, this::flatMap);
        }

        if (this.batchIterator.hasNext()) {
            assert this.head != null;
            E item = this.batchIterator.next();
            // TODO: find the parent node accurately instead the head
            return this.head.split(item, this);
        }

        throw FastNoSuchElementException.instance();
    }

    @Override
    public void reset() {
        super.reset();
        this.closeIterator();
        this.batchIterator = null;
        this.head = null;
    }

    @Override
    public Iterator<?> lastTimeResults() {
        /*
         * NOTE: fetch page from this iterator， can only get page info of
         * the lowest level, may lost info of upper levels.
         */
        return this.iterator;
    }

    @Override
    protected void closeIterator() {
        CloseableIterator.closeIterator(this.batchIterator);
    }

    @SuppressWarnings("unchecked")
    private Iterator<E> flatMap(List<Traverser.Admin<Vertex>> traversers) {
        if (this.head == null && traversers.size() > 0) {
            this.head = traversers.get(0);
        }
        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        if (queryVertex) {
            this.iterator = (Iterator<E>) this.vertices(traversers);
        } else {
            assert queryEdge;
            this.iterator = (Iterator<E>) this.edges(traversers);
        }
        return this.iterator;
    }

    private Iterator<Vertex> vertices(
                             List<Traverser.Admin<Vertex>> traversers) {
        assert traversers.size() > 0;

        Iterator<Edge> edges = this.edges(traversers);
        return this.queryAdjacentVertices(edges);
    }

    private Iterator<Edge> edges(List<Traverser.Admin<Vertex>> traversers) {
        assert traversers.size() > 0;

        BatchConditionQuery batchQuery = new BatchConditionQuery(
                                         VortexType.EDGE, traversers.size());

        for (Traverser.Admin<Vertex> traverser : traversers) {
            ConditionQuery query = this.constructEdgesQuery(traverser);
            /*
             * Merge each query into batch query through IN condition
             * NOTE: duplicated results may be removed by backend store
             */
            batchQuery.mergeToIN(query, VortexKeys.OWNER_VERTEX);
        }

        this.injectQueryInfo(batchQuery);
        return this.queryEdges(batchQuery);
    }
}
