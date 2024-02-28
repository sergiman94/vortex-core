
package com.vortex.vortexdb.backend.query;

import com.vortex.vortexdb.backend.query.Condition.RelationType;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.util.E;
import com.vortex.common.util.InsertionOrderUtil;

import java.util.ArrayList;
import java.util.List;

public class BatchConditionQuery extends ConditionQuery {

    private Condition.Relation in;
    private final int batchSize;

    public BatchConditionQuery(VortexType resultType, int batchSize) {
        super(resultType);
        this.in = null;
        this.batchSize = batchSize;
    }

    public void mergeToIN(ConditionQuery query, VortexKeys key) {
        Object value = query.condition(key);
        if (this.in == null) {
            assert !this.containsRelation(RelationType.IN);
            this.resetConditions(InsertionOrderUtil.newList(
                                 (List<Condition>) query.conditions()));
            this.unsetCondition(key);

            List<Object> list = new ArrayList<>(this.batchSize);
            list.add(value);
            // TODO: ensure not flatten BatchQuery
            this.in = (Condition.Relation) Condition.in(key, list);
            this.query(this.in);
        } else {
            E.checkArgument(this.in.key().equals(key),
                            "Invalid key '%s'", key);
            E.checkArgument(this.sameQueryExceptKeyIN(query),
                            "Can't merge query with different keys");

            @SuppressWarnings("unchecked")
            List<Object> values = ((List<Object>) this.in.value());
            values.add(value);
        }
    }

    protected boolean sameQueryExceptKeyIN(ConditionQuery query) {
        List<Condition.Relation> relations = query.relations();
        if (relations.size() != this.relations().size()) {
            return false;
        }

        for (Condition.Relation r : this.relations()) {
            if (r.relation() == RelationType.IN) {
                continue;
            }
            Object key = r.key();
            if (!this.condition(key).equals(query.condition(key))) {
                return false;
            }
        }
        return true;
    }
}
