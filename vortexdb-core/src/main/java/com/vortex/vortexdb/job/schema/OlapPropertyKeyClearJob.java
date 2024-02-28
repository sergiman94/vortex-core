
package com.vortex.vortexdb.job.schema;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.type.define.SchemaStatus;
import com.vortex.vortexdb.util.LockUtil;

public class OlapPropertyKeyClearJob extends IndexLabelRemoveJob {

    @Override
    public String type() {
        return CLEAR_OLAP;
    }

    @Override
    public Object execute() {
        Id olap = this.schemaId();

        // Clear olap data table
        this.params().graphTransaction().clearOlapPk(olap);

        // Clear corresponding index data
        clearIndexLabel(this.params(), olap);
        return null;
    }

    protected static void clearIndexLabel(VortexParams graph, Id id) {
        Id olapIndexLabel = findOlapIndexLabel(graph, id);
        if (olapIndexLabel == null) {
            return;
        }
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        IndexLabel indexLabel = schemaTx.getIndexLabel(olapIndexLabel);
        // If the index label does not exist, return directly
        if (indexLabel == null) {
            return;
        }
        LockUtil.Locks locks = new LockUtil.Locks(graph.name());
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, olapIndexLabel);
            // Set index label to "rebuilding" status
            schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.REBUILDING);
            try {
                // Remove index data
                graphTx.removeIndex(indexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.CREATED);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.INVALID);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }

    protected static Id findOlapIndexLabel(VortexParams graph, Id olap) {
        SchemaTransaction schemaTx = graph.schemaTransaction();
        for (IndexLabel indexLabel : schemaTx.getIndexLabels()) {
            if (indexLabel.indexFields().contains(olap)) {
                return indexLabel.id();
            }
        }
        return null;
    }
}
