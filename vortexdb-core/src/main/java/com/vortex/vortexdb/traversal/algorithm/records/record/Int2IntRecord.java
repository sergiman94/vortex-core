
package com.vortex.vortexdb.traversal.algorithm.records.record;

import com.vortex.vortexdb.util.collection.CollectionFactory;
import com.vortex.vortexdb.util.collection.IntIterator;
import com.vortex.vortexdb.util.collection.IntMap;

public class Int2IntRecord implements Record {

    private final IntMap layer;

    public Int2IntRecord() {
        this.layer = CollectionFactory.newIntMap();
    }

    @Override
    public IntIterator keys() {
        return this.layer.keys();
    }

    @Override
    public boolean containsKey(int node) {
        return this.layer.containsKey(node);
    }

    @Override
    public IntIterator get(int node) {
        int value = this.layer.get(node);
        if (value == IntMap.NULL_VALUE) {
            return IntIterator.EMPTY;
        }
        return IntIterator.wrap(value);
    }

    @Override
    public void addPath(int node, int parent) {
        this.layer.put(node, parent);
    }

    @Override
    public int size() {
        return this.layer.size();
    }

    @Override
    public boolean concurrent() {
        return this.layer.concurrent();
    }

    public IntMap layer() {
        return this.layer;
    }

    @Override
    public String toString() {
        return this.layer.toString();
    }
}
