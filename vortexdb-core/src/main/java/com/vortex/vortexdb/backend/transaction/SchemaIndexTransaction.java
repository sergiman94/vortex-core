
package com.vortex.vortexdb.backend.transaction;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.IdQuery;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendStore;
import com.vortex.common.perf.PerfUtil.Watched;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.structure.VortexIndex;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;

public class SchemaIndexTransaction extends AbstractTransaction {

    public SchemaIndexTransaction(VortexParams graph, BackendStore store) {
        super(graph, store);
    }

    @Watched(prefix = "index")
    public void updateNameIndex(SchemaElement element, boolean removed) {
        if (!this.needIndexForName()) {
            return;
        }

        IndexLabel indexLabel = IndexLabel.label(element.type());
        // Update name index if backend store not supports name-query
        VortexIndex index = new VortexIndex(this.graph(), indexLabel);
        index.fieldValues(element.name());
        index.elementIds(element.id());

        if (removed) {
            this.doEliminate(this.serializer.writeIndex(index));
        } else {
            this.doAppend(this.serializer.writeIndex(index));
        }
    }

    private boolean needIndexForName() {
        return !this.store().features().supportsQuerySchemaByName();
    }

    @Watched(prefix = "index")
    @Override
    public QueryResults<BackendEntry> query(Query query) {
        if (query instanceof ConditionQuery) {
            ConditionQuery q = (ConditionQuery) query;
            if (q.allSysprop() && q.conditionsSize() == 1 &&
                q.containsCondition(VortexKeys.NAME)) {
                return this.queryByName(q);
            }
        }
        return super.query(query);
    }

    @Watched(prefix = "index")
    private QueryResults<BackendEntry> queryByName(ConditionQuery query) {
        if (!this.needIndexForName()) {
            return super.query(query);
        }
        IndexLabel il = IndexLabel.label(query.resultType());
        String name = (String) query.condition(VortexKeys.NAME);
        E.checkState(name != null, "The name in condition can't be null " +
                     "when querying schema by name");

        ConditionQuery indexQuery;
        indexQuery = new ConditionQuery(VortexType.SECONDARY_INDEX, query);
        indexQuery.eq(VortexKeys.FIELD_VALUES, name);
        indexQuery.eq(VortexKeys.INDEX_LABEL_ID, il.id());

        IdQuery idQuery = new IdQuery(query.resultType(), query);
        Iterator<BackendEntry> entries = super.query(indexQuery).iterator();
        try {
            while (entries.hasNext()) {
                VortexIndex index = this.serializer.readIndex(graph(), indexQuery,
                                                            entries.next());
                idQuery.query(index.elementIds());
                Query.checkForceCapacity(idQuery.idsSize());
            }
        } finally {
            CloseableIterator.closeIterator(entries);
        }

        if (idQuery.ids().isEmpty()) {
            return QueryResults.empty();
        }

        assert idQuery.idsSize() == 1 : idQuery.ids();
        if (idQuery.idsSize() > 1) {
            LOG.warn("Multiple ids are found with same name '{}': {}",
                     name, idQuery.ids());
        }
        return super.query(idQuery);
    }
}
