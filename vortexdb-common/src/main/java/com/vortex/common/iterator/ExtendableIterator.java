package com.vortex.common.iterator;

import com.vortex.common.util.E;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ExtendableIterator<T> extends WrappedIterator<T> {

    private final Deque<Iterator<T>> itors;

    private Iterator<T> currentIterator;

    public ExtendableIterator() {
        this.itors = new ConcurrentLinkedDeque<>();
        this.currentIterator = null;
    }

    public ExtendableIterator(Iterator<T> iter) {
        this();
        this.extend(iter);
    }

    public ExtendableIterator(Iterator<T> itor1, Iterator<T> itor2) {
        this();
        this.extend(itor1);
        this.extend(itor2);
    }

    public ExtendableIterator<T> extend(Iterator<T> iter) {
        E.checkState(this.currentIterator == null, "Can't extend iterator after iterating");

        if (iter != null) {
            this.itors.addLast(iter);
        }

        return this;
    }

    @Override
    public void close() throws Exception {
        for (Iterator<T> iter : this.itors) {
            if (iter instanceof AutoCloseable) {
                ((AutoCloseable) iter).close();
            }
        }
    }

    @Override
    protected Iterator<?> originIterator() {
        return this.currentIterator;
    }

    @Override
    protected boolean fetch() {
        assert this.current == none();

        if (this.itors.isEmpty()) {
            return false;
        }

        if (this.currentIterator != null && this.currentIterator.hasNext()) {
            this.current = this.currentIterator.next();
            return true;
        }

        Iterator<T> first = null;
        while ((first = this.itors.peekFirst()) != null && !first.hasNext()) {
            if(first == this.itors.peekLast() && this.itors.size() == 1) {
                this.currentIterator = first;
                //the last one haha
                return false;
            }

            close(this.itors.removeFirst());
        }

        assert first != null && first.hasNext();
        this.currentIterator = first;
        this.current = this.currentIterator.next();
        return true;
    }
}
