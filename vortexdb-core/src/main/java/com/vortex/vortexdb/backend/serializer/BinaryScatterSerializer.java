
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendEntry.BackendColumn;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.structure.VortexProperty;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.structure.VortexVertexProperty;
import com.vortex.vortexdb.type.define.VortexKeys;

public class BinaryScatterSerializer extends BinarySerializer {

    public BinaryScatterSerializer() {
        super(true, true);
    }

    @Override
    public BackendEntry writeVertex(VortexVertex vertex) {
        BinaryBackendEntry entry = newBackendEntry(vertex);

        if (vertex.removed()) {
            return entry;
        }

        // Write vertex label
        entry.column(this.formatLabel(vertex));

        // Write all properties of a Vertex
        for (VortexProperty<?> prop : vertex.getProperties()) {
            entry.column(this.formatProperty(prop));
        }

        return entry;
    }

    @Override
    public VortexVertex readVertex(Vortex graph, BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);

        // Parse label
        final byte[] VL = this.formatSyspropName(entry.id(), VortexKeys.LABEL);
        BackendColumn vl = entry.column(VL);
        VertexLabel vertexLabel = VertexLabel.NONE;
        if (vl != null) {
            Id labelId = BytesBuffer.wrap(vl.value).readId();
            vertexLabel = graph.vertexLabelOrNone(labelId);
        }

        // Parse id
        Id id = entry.id().origin();
        VortexVertex vertex = new VortexVertex(graph, id, vertexLabel);

        // Parse all properties and edges of a Vertex
        for (BackendColumn col : entry.columns()) {
            this.parseColumn(col, vertex);
        }

        return vertex;
    }

    @Override
    public BackendEntry writeVertexProperty(VortexVertexProperty<?> prop) {
        BinaryBackendEntry entry = newBackendEntry(prop.element());
        entry.column(this.formatProperty(prop));
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }
}
