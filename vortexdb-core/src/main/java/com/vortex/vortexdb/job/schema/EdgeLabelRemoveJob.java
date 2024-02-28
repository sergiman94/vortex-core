
package com.vortex.vortexdb.job.schema;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.type.define.SchemaStatus;
import com.vortex.vortexdb.util.LockUtil;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class EdgeLabelRemoveJob extends SchemaJob {

    @Override
    public String type() {
        return REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeEdgeLabel(this.params(), this.schemaId());
        return null;
    }

    private static void removeEdgeLabel(VortexParams graph, Id id) {
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        EdgeLabel edgeLabel = schemaTx.getEdgeLabel(id);
        // If the edge label does not exist, return directly
        if (edgeLabel == null) {
            return;
        }
        if (edgeLabel.status().deleting()) {
            LOG.info("The edge label '{}' has been in {} status, " +
                     "please check if it's expected to delete it again",
                     edgeLabel, edgeLabel.status());
        }
        // Remove index related data(include schema) of this edge label
        Set<Id> indexIds = ImmutableSet.copyOf(edgeLabel.indexLabels());
        LockUtil.Locks locks = new LockUtil.Locks(graph.name());
        try {
            locks.lockWrites(LockUtil.EDGE_LABEL_DELETE, id);
            schemaTx.updateSchemaStatus(edgeLabel, SchemaStatus.DELETING);
            try {
                for (Id indexId : indexIds) {
                    IndexLabelRemoveJob.removeIndexLabel(graph, indexId);
                }
                // Remove all edges which has matched label
                // TODO: use event to replace direct call
                graphTx.removeEdges(edgeLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                // Remove edge label
                removeSchema(schemaTx, edgeLabel);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(edgeLabel, SchemaStatus.UNDELETED);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }
}
