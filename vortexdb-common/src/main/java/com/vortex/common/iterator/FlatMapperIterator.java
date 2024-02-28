package com.vortex.common.iterator;

import com.vortex.common.util.E;

import java.util.Iterator;
import java.util.function.Function;

public class FlatMapperIterator<T, R> extends WrappedIterator<R> {

    private final Iterator<T> originIterator;
    private final Function<T, Iterator<R>> mapperCallback;

    protected Iterator<R> batchIterator;

    public FlatMapperIterator(Iterator<T> origin,
                              Function<T, Iterator<R>> mapper) {
        this.originIterator = origin;
        this.mapperCallback = mapper;
        this.batchIterator = null;
    }

    @Override
    public void close() throws Exception {
        this.resetBatchIterator();
        super.close();
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }

    @Override
    protected final boolean fetch() {
        if (this.batchIterator != null && this.fetchFromBatch()) {
            return true;
        }

        while (this.originIterator.hasNext()) {
            T next = this.originIterator.next();
            assert this.batchIterator == null;
            // Do fetch
            this.batchIterator = this.mapperCallback.apply(next);
            if (this.batchIterator != null && this.fetchFromBatch()) {
                return true;
            }
        }
        return false;
    }

    protected boolean fetchFromBatch() {
        E.checkNotNull(this.batchIterator, "mapper results");
        while (this.batchIterator.hasNext()) {
            R result = this.batchIterator.next();
            if (result != null) {
                assert this.current == none();
                this.current = result;
                return true;
            }
        }
        this.resetBatchIterator();
        return false;
    }

    protected final void resetBatchIterator() {
        if (this.batchIterator == null) {
            return;
        }
        close(this.batchIterator);
        this.batchIterator = null;
    }
}
