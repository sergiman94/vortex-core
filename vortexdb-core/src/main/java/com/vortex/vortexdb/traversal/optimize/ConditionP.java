
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.vortexdb.backend.query.Condition.RelationType;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

public class ConditionP extends P<Object> {

    private static final long serialVersionUID = 9094970577400072902L;

    private ConditionP(final BiPredicate<Object, Object> predicate,
                       Object value) {
        super(predicate, value);
    }

    public static ConditionP textContains(Object value) {
        return new ConditionP(RelationType.TEXT_CONTAINS, value);
    }

    public static ConditionP contains(Object value) {
        return new ConditionP(RelationType.CONTAINS, value);
    }

    public static ConditionP containsK(Object value) {
        return new ConditionP(RelationType.CONTAINS_KEY, value);
    }

    public static ConditionP containsV(Object value) {
        return new ConditionP(RelationType.CONTAINS_VALUE, value);
    }

    public static ConditionP eq(Object value) {
        // EQ that can compare two array
        return new ConditionP(RelationType.EQ, value);
    }
}
