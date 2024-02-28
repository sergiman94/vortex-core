
package com.vortex.vortexdb.schema.builder;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.exception.ExistedException;
import com.vortex.vortexdb.schema.*;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.GraphMode;
import com.vortex.vortexdb.type.define.SchemaStatus;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.LockUtil;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;

import java.util.Set;
import java.util.function.Function;

public abstract class AbstractBuilder {

    private final SchemaTransaction transaction;
    private final Vortex graph;

    public AbstractBuilder(SchemaTransaction transaction, Vortex graph) {
        E.checkNotNull(transaction, "transaction");
        E.checkNotNull(graph, "graph");
        this.transaction = transaction;
        this.graph = graph;
    }

    protected Vortex graph() {
        return this.graph;
    }

    protected Id validOrGenerateId(VortexType type, Id id, String name) {
        return this.transaction.validOrGenerateId(type, id, name);
    }

    protected void checkSchemaName(String name) {
        this.transaction.checkSchemaName(name);
    }

    protected Id rebuildIndex(IndexLabel indexLabel, Set<Id> dependencies) {
        return this.transaction.rebuildIndex(indexLabel, dependencies);
    }

    protected <V> V lockCheckAndCreateSchema(VortexType type, String name,
                                             Function<String, V> callback) {
        String graph = this.transaction.graphName();
        LockUtil.Locks locks = new LockUtil.Locks(graph);
        try {
            locks.lockWrites(LockUtil.vortexType2Group(type),
                             IdGenerator.of(name));
            return callback.apply(name);
        } finally {
            locks.unlock();
        }
    }

    protected void updateSchemaStatus(SchemaElement element,
                                      SchemaStatus status) {
        this.transaction.updateSchemaStatus(element, status);
    }

    protected void checkSchemaIdIfRestoringMode(VortexType type, Id id) {
        if (this.transaction.graphMode() == GraphMode.RESTORING) {
            E.checkArgument(id != null,
                            "Must provide schema id if in RESTORING mode");
            if (this.transaction.existsSchemaId(type, id)) {
                throw new ExistedException(type.readableName() + " id", id);
            }
        }
    }

    protected PropertyKey propertyKeyOrNull(String name) {
        return this.transaction.getPropertyKey(name);
    }

    protected VertexLabel vertexLabelOrNull(String name) {
        return this.transaction.getVertexLabel(name);
    }

    protected EdgeLabel edgeLabelOrNull(String name) {
        return this.transaction.getEdgeLabel(name);
    }

    protected IndexLabel indexLabelOrNull(String name) {
        return this.transaction.getIndexLabel(name);
    }
}
