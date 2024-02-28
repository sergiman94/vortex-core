
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.vortexdb.backend.query.Aggregate.AggregateFunc;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.common.iterator.Metadatable;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;

import java.util.Iterator;

public interface QueryHolder extends HasContainerHolder, Metadatable {

    public static final String SYSPROP_PAGE = "~page";

    public Iterator<?> lastTimeResults();

    @Override
    public default Object metadata(String meta, Object... args) {
        Iterator<?> results = this.lastTimeResults();
        if (results instanceof Metadatable) {
            return ((Metadatable) results).metadata(meta, args);
        }
        throw new IllegalStateException("Original results is not Metadatable");
    }

    public Query queryInfo();

    public default void orderBy(String key, Order order) {
        this.queryInfo().order(TraversalUtil.string2vortexKey(key),
                               TraversalUtil.convOrder(order));
    }

    public default long setRange(long start, long end) {
        return this.queryInfo().range(start, end);
    }

    public default void setPage(String page) {
        this.queryInfo().page(page);
    }

    public default void setCount() {
        this.queryInfo().capacity(Query.NO_CAPACITY);
    }

    public default void setAggregate(AggregateFunc func, String key) {
        this.queryInfo().aggregate(func, key);
    }

    public default <Q extends Query> Q injectQueryInfo(Q query) {
        query.copyBasic(this.queryInfo());
        return query;
    }
}
