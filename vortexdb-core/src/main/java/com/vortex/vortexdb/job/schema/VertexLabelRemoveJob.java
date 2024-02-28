
package com.vortex.vortexdb.job.schema;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.type.define.SchemaStatus;
import com.vortex.vortexdb.util.LockUtil;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public class VertexLabelRemoveJob extends SchemaJob {

    @Override
    public String type() {
        return REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeVertexLabel(this.params(), this.schemaId());
        return null;
    }

    private static void removeVertexLabel(VortexParams graph, Id id) {
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        VertexLabel vertexLabel = schemaTx.getVertexLabel(id);
        // If the vertex label does not exist, return directly
        if (vertexLabel == null) {
            return;
        }
        if (vertexLabel.status().deleting()) {
            LOG.info("The vertex label '{}' has been in {} status, " +
                     "please check if it's expected to delete it again",
                     vertexLabel, vertexLabel.status());
        }

        // Check no edge label use the vertex label
        List<EdgeLabel> edgeLabels = schemaTx.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(id)) {
                throw new VortexException(
                          "Not allowed to remove vertex label '%s' " +
                          "because the edge label '%s' still link with it",
                          vertexLabel.name(), edgeLabel.name());
            }
        }

        /*
         * Copy index label ids because removeIndexLabel will mutate
         * vertexLabel.indexLabels()
         */
        Set<Id> indexLabelIds = ImmutableSet.copyOf(vertexLabel.indexLabels());
        LockUtil.Locks locks = new LockUtil.Locks(graph.name());
        try {
            locks.lockWrites(LockUtil.VERTEX_LABEL_DELETE, id);
            schemaTx.updateSchemaStatus(vertexLabel, SchemaStatus.DELETING);
            try {
                for (Id ilId : indexLabelIds) {
                    IndexLabelRemoveJob.removeIndexLabel(graph, ilId);
                }
                // TODO: use event to replace direct call
                // Deleting a vertex will automatically deletes the held edge
                graphTx.removeVertices(vertexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                // Remove vertex label
                removeSchema(schemaTx, vertexLabel);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(vertexLabel,
                                            SchemaStatus.UNDELETED);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }
}
