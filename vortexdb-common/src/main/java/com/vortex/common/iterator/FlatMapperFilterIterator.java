
package com.vortex.common.iterator;

import com.vortex.common.util.E;

import java.util.Iterator;
import java.util.function.Function;

public class FlatMapperFilterIterator<T, R> extends FlatMapperIterator<T, R> {

    private final Function<R, Boolean> filterCallback;

    public FlatMapperFilterIterator(Iterator<T> origin,
                                    Function<T, Iterator<R>> mapper,
                                    Function<R, Boolean> filter) {
        super(origin, mapper);
        this.filterCallback = filter;
    }

    @Override
    protected final boolean fetchFromBatch() {
        E.checkNotNull(this.batchIterator, "mapper results");
        while (this.batchIterator.hasNext()) {
            R result = this.batchIterator.next();
            if (result != null && this.filterCallback.apply(result)) {
                assert this.current == none();
                this.current = result;
                return true;
            }
        }
        this.resetBatchIterator();
        return false;
    }
}
