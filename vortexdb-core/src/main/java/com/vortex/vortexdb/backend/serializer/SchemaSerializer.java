
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.VertexLabel;

public interface SchemaSerializer {

    public BackendEntry writeVertexLabel(VertexLabel vertexLabel);
    public VertexLabel readVertexLabel(Vortex graph, BackendEntry entry);

    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel);
    public EdgeLabel readEdgeLabel(Vortex graph, BackendEntry entry);

    public BackendEntry writePropertyKey(PropertyKey propertyKey);
    public PropertyKey readPropertyKey(Vortex graph, BackendEntry entry);

    public BackendEntry writeIndexLabel(IndexLabel indexLabel);
    public IndexLabel readIndexLabel(Vortex graph, BackendEntry entry);
}
