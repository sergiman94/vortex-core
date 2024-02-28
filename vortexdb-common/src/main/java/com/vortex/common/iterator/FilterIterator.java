
package com.vortex.common.iterator;

import java.util.Iterator;
import java.util.function.Function;

public class FilterIterator<T> extends WrappedIterator<T> {

    private final Iterator<T> originIterator;
    private final Function<T, Boolean> filterCallback;

    public FilterIterator(Iterator<T> origin, Function<T, Boolean> filter) {
        this.originIterator = origin;
        this.filterCallback = filter;
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }

    @Override
    protected final boolean fetch() {
        while (this.originIterator.hasNext()) {
            T next = this.originIterator.next();
            // Do filter
            if (next != null && this.filterCallback.apply(next)) {
                assert this.current == none();
                this.current = next;
                return true;
            }
        }
        return false;
    }
}
