
package com.vortex.vortexdb.job.system;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.common.util.E;

import java.util.Set;

public class DeleteExpiredElementJob<V> extends DeleteExpiredJob<V> {

    private static final String JOB_TYPE = "delete_expired_element";

    private Set<VortexElement> elements;

    public DeleteExpiredElementJob(Set<VortexElement> elements) {
        E.checkArgument(elements != null && !elements.isEmpty(),
                        "The element can't be null or empty");
        this.elements = elements;
    }

    @Override
    public String type() {
        return JOB_TYPE;
    }

    @Override
    public V execute() throws Exception {
        LOG.debug("Delete expired elements: {}", this.elements);

        VortexParams graph = this.params();
        GraphTransaction tx = graph.graphTransaction();
        try {
            for (VortexElement element : this.elements) {
                element.remove();
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            LOG.warn("Failed to delete expired elements: {}", this.elements);
            throw e;
        } finally {
            JOB_COUNTERS.jobCounter(graph.graph()).decrement();
        }
        return null;
    }
}
