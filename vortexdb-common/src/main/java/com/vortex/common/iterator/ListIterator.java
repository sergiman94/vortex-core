
package com.vortex.common.iterator;

import com.vortex.common.util.InsertionOrderUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ListIterator<T> extends WrappedIterator<T> {

    private final Iterator<T> originIterator;
    private final Iterator<T> resultsIterator;
    private final Collection<T> results;

    public ListIterator(long capacity, Iterator<T> origin) {
        List<T> results = InsertionOrderUtil.newList();
        while (origin.hasNext()) {
            if (capacity >= 0L && results.size() >= capacity) {
                throw new IllegalArgumentException(
                          "The iterator exceeded capacity " + capacity);
            }
            results.add(origin.next());
        }
        this.originIterator = origin;
        this.results = Collections.unmodifiableList(results);
        this.resultsIterator = this.results.iterator();
    }

    public ListIterator(Collection<T> origin) {
        this.originIterator = origin.iterator();
        this.results = origin instanceof List ?
                       Collections.unmodifiableList((List<T>) origin) :
                       Collections.unmodifiableCollection(origin);
        this.resultsIterator = this.results.iterator();
    }

    @Override
    public void remove() {
        this.resultsIterator.remove();
    }

    public Collection<T> list() {
        return this.results;
    }

    @Override
    protected boolean fetch() {
        assert this.current == none();
        if (!this.resultsIterator.hasNext()) {
            return false;
        }
        this.current = this.resultsIterator.next();
        return true;
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }
}
