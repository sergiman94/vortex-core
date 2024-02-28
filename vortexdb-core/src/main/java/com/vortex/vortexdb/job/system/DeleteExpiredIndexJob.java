
package com.vortex.vortexdb.job.system;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.query.IdQuery;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.structure.VortexIndex;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;

import java.util.Iterator;
import java.util.Set;

public class DeleteExpiredIndexJob<V> extends DeleteExpiredJob<V> {

    private static final String JOB_TYPE = "delete_expired_index";

    private Set<VortexIndex> indexes;

    public DeleteExpiredIndexJob(Set<VortexIndex> indexes) {
        E.checkArgument(indexes != null && !indexes.isEmpty(),
                        "The indexes can't be null or empty");
        this.indexes = indexes;
    }

    @Override
    public String type() {
        return JOB_TYPE;
    }

    @Override
    public V execute() throws Exception {
        LOG.debug("Delete expired indexes: {}", this.indexes);

        VortexParams graph = this.params();
        GraphTransaction tx = graph.graphTransaction();
        try {
            for (VortexIndex index : this.indexes) {
                this.deleteExpiredIndex(graph, index);
            }
            tx.commit();
        } catch (Throwable e) {
            tx.rollback();
            LOG.warn("Failed to delete expired indexes: {}", this.indexes);
            throw e;
        } finally {
            JOB_COUNTERS.jobCounter(graph.graph()).decrement();
        }
        return null;
    }

    /*
     * Delete expired element(if exist) of the index,
     * otherwise just delete expired index only
     */
    private void deleteExpiredIndex(VortexParams graph, VortexIndex index) {
        GraphTransaction tx = graph.graphTransaction();
        VortexType type = index.indexLabel().queryType().isVertex()?
                        VortexType.VERTEX : VortexType.EDGE;
        IdQuery query = new IdQuery(type);
        query.query(index.elementId());
        query.showExpired(true);
        Iterator<?> elements = type.isVertex() ?
                               tx.queryVertices(query) :
                               tx.queryEdges(query);
        if (elements.hasNext()) {
            VortexElement element = (VortexElement) elements.next();
            if (element.expiredTime() == index.expiredTime()) {
                element.remove();
            } else {
                tx.removeIndex(index);
            }
        } else {
            tx.removeIndex(index);
        }
    }
}
