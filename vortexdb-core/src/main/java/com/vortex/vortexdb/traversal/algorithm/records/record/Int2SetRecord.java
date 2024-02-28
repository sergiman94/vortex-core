
package com.vortex.vortexdb.traversal.algorithm.records.record;

import com.vortex.vortexdb.util.collection.IntIterator;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

public class Int2SetRecord implements Record {

    private final IntObjectHashMap<IntHashSet> layer;

    public Int2SetRecord() {
        this.layer = new IntObjectHashMap<>();
    }

    @Override
    public IntIterator keys() {
        return IntIterator.wrap(this.layer.keySet().intIterator());
    }

    @Override
    public boolean containsKey(int node) {
        return this.layer.containsKey(node);
    }

    @Override
    public IntIterator get(int node) {
        return IntIterator.wrap(this.layer.get(node).intIterator());
    }

    @Override
    public void addPath(int node, int parent) {
        IntHashSet values = this.layer.get(node);
        if (values != null) {
            values.add(parent);
        } else {
            // TODO: use one sorted-array instead to store all values
            this.layer.put(node, IntHashSet.newSetWith(parent));
        }
    }

    @Override
    public int size() {
        return this.layer.size();
    }

    @Override
    public boolean concurrent() {
        return false;
    }

    @Override
    public String toString() {
        return this.layer.toString();
    }
}
