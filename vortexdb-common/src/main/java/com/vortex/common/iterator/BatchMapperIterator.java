
package com.vortex.common.iterator;

import com.google.common.collect.ImmutableList;
import com.vortex.common.util.E;
import com.vortex.common.util.InsertionOrderUtil;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class BatchMapperIterator<T, R> extends WrappedIterator<R> {

    private final int batch;
    private final Iterator<T> originIterator;
    private final Function<List<T>, Iterator<R>> mapperCallback;

    private Iterator<R> batchIterator;

    public BatchMapperIterator(int batch, Iterator<T> origin,
                               Function<List<T>, Iterator<R>> mapper) {
        E.checkArgument(batch > 0, "Expect batch > 0, but got %s", batch);
        this.batch = batch;
        this.originIterator = origin;
        this.mapperCallback = mapper;
        this.batchIterator = null;
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

        List<T> batch = this.nextBatch();
        assert this.batchIterator == null;
        while (!batch.isEmpty()) {
            // Do fetch
            this.batchIterator = this.mapperCallback.apply(batch);
            if (this.batchIterator != null && this.fetchFromBatch()) {
                return true;
            }
            // Try next batch
            batch = this.nextBatch();
        }
        return false;
    }

    protected final List<T> nextBatch() {
        if (!this.originIterator.hasNext()) {
            return ImmutableList.of();
        }
        List<T> list = InsertionOrderUtil.newList();
        for (int i = 0; i < this.batch && this.originIterator.hasNext(); i++) {
            T next = this.originIterator.next();
            list.add(next);
        }
        return list;
    }

    protected final boolean fetchFromBatch() {
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
