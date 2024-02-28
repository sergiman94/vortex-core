package com.vortex.common.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class WrappedIterator<R> implements CIter<R> {

    private static final Object NONE = new Object();

    protected R current;

    public WrappedIterator() {this.current = none();}

    @Override
    public void remove() {
        Iterator<?> iterator = this.originIterator();
        if (iterator == null) {
            throw new NoSuchElementException("The Origin iterator can't be null for removing");
        }

        iterator.remove();
    }

    @Override
    public Object metadata(String meta, Object... args) {
        Iterator<?> iterator =  this.originIterator();

        if(iterator instanceof Metadatable) {
            return ((Metadatable) iterator).metadata(meta, args);
        }

        throw new IllegalStateException("Original iterator is not Metadatable");
    }

    @Override
    public void close() throws Exception {
        Iterator<?> iterator = this.originIterator();
        if (iterator instanceof AutoCloseable) {
            ((AutoCloseable) iterator).close();
        }
    }

    public static void close(Iterator<?> iterator) {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (Exception e ) {
                throw new IllegalStateException("Failed to close iterator");
            }
        }
    }

    @Override
    public boolean hasNext() {
        if(this.current != none()) {
            return true;
        }

        return  this.fetch();
    }

    @Override
    public R next() {
        if (this.current == none()) {
            this.fetch();
            if (this.current == none()) {
                throw new NoSuchElementException();
            }
        }

        R current = this.current;
        this.current = none();
        return current;
    }

    @SuppressWarnings("unchecked")
    protected static final <R> R none() {return (R) NONE;}

    protected  abstract  Iterator<?> originIterator();

    protected  abstract boolean fetch();
}
