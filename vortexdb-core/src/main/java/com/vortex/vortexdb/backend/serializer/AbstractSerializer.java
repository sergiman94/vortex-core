
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.IdQuery;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.type.VortexType;

public abstract class AbstractSerializer
                implements GraphSerializer, SchemaSerializer {

    protected BackendEntry convertEntry(BackendEntry entry) {
        return entry;
    }

    protected abstract BackendEntry newBackendEntry(VortexType type, Id id);

    protected abstract Id writeQueryId(VortexType type, Id id);

    protected abstract Query writeQueryEdgeCondition(Query query);

    protected abstract Query writeQueryCondition(Query query);

    @Override
    public Query writeQuery(Query query) {
        VortexType type = query.resultType();

        // Serialize edge condition query (TODO: add VEQ(for EOUT/EIN))
        if (type.isEdge() && query.conditionsSize() > 0) {
            if (query.idsSize() > 0) {
                throw new BackendException("Not supported query edge by id " +
                                           "and by condition at the same time");
            }

            Query result = this.writeQueryEdgeCondition(query);
            if (result != null) {
                return result;
            }
        }

        // Serialize id in query
        if (query.idsSize() == 1 && query instanceof IdQuery.OneIdQuery) {
            IdQuery.OneIdQuery result = (IdQuery.OneIdQuery) query.copy();
            result.resetId(this.writeQueryId(type, result.id()));
            query = result;
        } else if (query.idsSize() > 0 && query instanceof IdQuery) {
            IdQuery result = (IdQuery) query.copy();
            result.resetIds();
            for (Id id : query.ids()) {
                result.query(this.writeQueryId(type, id));
            }
            query = result;
        }

        // Serialize condition(key/value) in query
        if (query instanceof ConditionQuery && query.conditionsSize() > 0) {
            query = this.writeQueryCondition(query);
        }

        return query;
    }
}
