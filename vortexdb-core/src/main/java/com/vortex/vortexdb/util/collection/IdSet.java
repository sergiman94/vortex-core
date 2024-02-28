
package com.vortex.vortexdb.util.collection;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.common.iterator.ExtendableIterator;
import com.vortex.vortexdb.type.define.CollectionType;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

public class IdSet extends AbstractSet<Id> {

    private final LongHashSet numberIds;
    private final Set<Id> nonNumberIds;

    public IdSet(CollectionType type) {
        this.numberIds = new LongHashSet();
        this.nonNumberIds = CollectionFactory.newSet(type);
    }

    @Override
    public int size() {
        return this.numberIds.size() + this.nonNumberIds.size();
    }

    @Override
    public boolean isEmpty() {
        return this.numberIds.isEmpty() && this.nonNumberIds.isEmpty();
    }

    @Override
    public boolean contains(Object object) {
        if (!(object instanceof Id)) {
            return false;
        }
        Id id = (Id) object;
        if (id.type() == Id.IdType.LONG) {
            return this.numberIds.contains(id.asLong());
        } else {
            return this.nonNumberIds.contains(id);
        }
    }

    @Override
    public Iterator<Id> iterator() {
        return new ExtendableIterator<>(
               this.nonNumberIds.iterator(),
               new EcLongIdIterator(this.numberIds.longIterator()));
    }

    @Override
    public boolean add(Id id) {
        if (id.type() == Id.IdType.LONG) {
            return this.numberIds.add(id.asLong());
        } else {
            return this.nonNumberIds.add(id);
        }
    }

    public boolean remove(Id id) {
        if (id.type() == Id.IdType.LONG) {
            return this.numberIds.remove(id.asLong());
        } else {
            return this.nonNumberIds.remove(id);
        }
    }

    @Override
    public void clear() {
        this.numberIds.clear();
        this.nonNumberIds.clear();
    }

    private static class EcLongIdIterator implements Iterator<Id> {

        private final MutableLongIterator iterator;

        public EcLongIdIterator(MutableLongIterator iter) {
            this.iterator = iter;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Id next() {
            return IdGenerator.of(this.iterator.next());
        }

        @Override
        public void remove() {
            this.iterator.remove();
        }
    }
}
