
package com.vortex.common.iterator;

import java.util.Iterator;
import java.util.function.Function;

public class LimitIterator<T> extends WrappedIterator<T> {

    private final Iterator<T> originIterator;
    private final Function<T, Boolean> filterCallback;

    public LimitIterator(Iterator<T> origin, Function<T, Boolean> filter) {
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
            if (next == null) {
                continue;
            }
            // Do filter
            boolean reachLimit = this.filterCallback.apply(next);
            if (reachLimit) {
                this.closeOriginIterator();
                return false;
            }
            assert this.current == none();
            this.current = next;
            return true;
        }
        return false;
    }

    protected final void closeOriginIterator() {
        if (this.originIterator == null) {
            return;
        }
        close(this.originIterator);
    }
}
