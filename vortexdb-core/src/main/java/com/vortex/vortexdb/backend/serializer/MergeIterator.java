
package com.vortex.vortexdb.backend.serializer;

import com.vortex.common.iterator.WrappedIterator;
import com.vortex.common.util.E;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

public class MergeIterator<T, R> extends WrappedIterator<T> {

    private final Iterator<T> originIterator;
    private final BiFunction<T, R, Boolean> merger;
    private final List<Iterator<R>> iterators = new ArrayList<>();
    private final List<R> headElements;

    public MergeIterator(Iterator<T> originIterator,
                         List<Iterator<R>> iterators,
                         BiFunction<T, R, Boolean> merger) {
        E.checkArgumentNotNull(originIterator, "The origin iterator of " +
                               "MergeIterator can't be null");
        E.checkArgument(iterators != null && !iterators.isEmpty(),
                        "The iterators of MergeIterator can't be " +
                        "null or empty");
        E.checkArgumentNotNull(merger, "The merger function of " +
                               "MergeIterator can't be null");
        this.originIterator = originIterator;
        this.headElements = new ArrayList<>();

        for (Iterator<R> iterator : iterators) {
            if (iterator.hasNext()) {
                this.iterators.add(iterator);
                this.headElements.add(iterator.next());
            }
        }

        this.merger = merger;
    }

    @Override
    public void close() throws Exception {
        for (Iterator<R> iter : this.iterators) {
            if (iter instanceof AutoCloseable) {
                ((AutoCloseable) iter).close();
            }
        }
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }

    @Override
    protected final boolean fetch() {
        if (!this.originIterator.hasNext()) {
            return false;
        }

        T next = this.originIterator.next();

        for (int i = 0; i < this.iterators.size(); i++) {
            R element = this.headElements.get(i);
            if (element == none()) {
                continue;
            }

            if (this.merger.apply(next, element)) {
                Iterator<R> iter = this.iterators.get(i);
                if (iter.hasNext()) {
                    this.headElements.set(i, iter.next());
                } else {
                    this.headElements.set(i, none());
                    close(iter);
                }
            }
        }

        assert this.current == none();
        this.current = next;
        return true;
    }
}
