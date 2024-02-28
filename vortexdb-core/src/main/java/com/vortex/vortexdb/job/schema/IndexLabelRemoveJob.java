
package com.vortex.vortexdb.job.schema;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.type.define.SchemaStatus;
import com.vortex.vortexdb.util.LockUtil;

public class IndexLabelRemoveJob extends SchemaJob {

    @Override
    public String type() {
        return REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeIndexLabel(this.params(), this.schemaId());
        return null;
    }

    protected static void removeIndexLabel(VortexParams graph, Id id) {
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        IndexLabel indexLabel = schemaTx.getIndexLabel(id);
        // If the index label does not exist, return directly
        if (indexLabel == null) {
            return;
        }
        if (indexLabel.status().deleting()) {
            LOG.info("The index label '{}' has been in {} status, " +
                     "please check if it's expected to delete it again",
                     indexLabel, indexLabel.status());
        }
        LockUtil.Locks locks = new LockUtil.Locks(graph.name());
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, id);
            // TODO add update lock
            // Set index label to "deleting" status
            schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.DELETING);
            try {
                // Remove label from indexLabels of vertex or edge label
                removeIndexLabelFromBaseLabel(schemaTx, indexLabel);
                // Remove index data
                // TODO: use event to replace direct call
                graphTx.removeIndex(indexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                // Remove index label
                removeSchema(schemaTx, indexLabel);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.UNDELETED);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }
}
