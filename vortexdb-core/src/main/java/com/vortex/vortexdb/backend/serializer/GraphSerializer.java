
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.structure.*;
import com.vortex.vortexdb.type.VortexType;

public interface GraphSerializer {

    public BackendEntry writeVertex(VortexVertex vertex);
    public BackendEntry writeOlapVertex(VortexVertex vertex);
    public BackendEntry writeVertexProperty(VortexVertexProperty<?> prop);
    public VortexVertex readVertex(Vortex graph, BackendEntry entry);

    public BackendEntry writeEdge(VortexEdge edge);
    public BackendEntry writeEdgeProperty(VortexEdgeProperty<?> prop);
    public VortexEdge readEdge(Vortex graph, BackendEntry entry);

    public BackendEntry writeIndex(VortexIndex index);
    public VortexIndex readIndex(Vortex graph, ConditionQuery query,
                               BackendEntry entry);

    public BackendEntry writeId(VortexType type, Id id);
    public Query writeQuery(Query query);
}
