
package com.vortex.common.iterator;

import java.util.Iterator;
import java.util.function.Function;

public class MapperIterator<T, R> extends WrappedIterator<R> {

    private final Iterator<T> originIterator;
    private final Function<T, R> mapperCallback;

    public MapperIterator(Iterator<T> origin, Function<T, R> mapper) {
        this.originIterator = origin;
        this.mapperCallback = mapper;
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }

    @Override
    protected final boolean fetch() {
        while (this.originIterator.hasNext()) {
            T next = this.originIterator.next();
            R result = this.mapperCallback.apply(next);
            if (result != null) {
                assert this.current == none();
                this.current = result;
                return true;
            }
        }
        return false;
    }
}
