
package com.vortex.vortexdb.backend.query;

import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.util.Iterator;
import java.util.function.BiFunction;

public class Aggregate {

    private final AggregateFunc func;
    private final String column;

    public Aggregate(AggregateFunc func, String column) {
        this.func = func;
        this.column = column;
    }

    public AggregateFunc func() {
        return this.func;
    }

    public String column() {
        return this.column;
    }

    public boolean countAll() {
        return this.func == AggregateFunc.COUNT && this.column == null;
    }

    public Number reduce(Iterator<Number> results) {
        return this.func.reduce(results);
    }

    public Number defaultValue() {
        return this.func.defaultValue();
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", this.func.string(),
                             this.column == null ? "*" : this.column);
    }

    public static enum AggregateFunc {

        COUNT("count", 0L, NumberHelper::add),
        MAX("max", -Double.MAX_VALUE, NumberHelper::max),
        MIN("min", Double.MAX_VALUE, NumberHelper::min),
        AVG("avg", 0D, NumberHelper::add),
        SUM("sum", 0L, NumberHelper::add);

        private final String name;
        private final Number defaultValue;
        private final BiFunction<Number, Number, Number> merger;

        private AggregateFunc(String name, Number defaultValue,
                              BiFunction<Number, Number, Number> merger) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.merger = merger;
        }

        public String string() {
            return this.name;
        }

        public Number defaultValue() {
            return this.defaultValue;
        }

        public Number reduce(Iterator<Number> results) {
            Number number;
            long count = 0L;
            if (results.hasNext()) {
                number = results.next();
                count++;
            } else {
                return this.defaultValue;
            }
            while (results.hasNext()) {
                number = this.merger.apply(number, results.next());
                count++;
            }
            if (this == AVG) {
                number = NumberHelper.div(number, count);
            }
            return number;
        }
    }
}
