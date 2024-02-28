
package com.vortex.vortexdb.traversal.algorithm.records.record;

import com.vortex.vortexdb.util.collection.Int2IntsMap;
import com.vortex.vortexdb.util.collection.IntIterator;

public class Int2ArrayRecord implements Record {

    private final Int2IntsMap layer;

    public Int2ArrayRecord() {
        this.layer = new Int2IntsMap();
    }

    @Override
    public IntIterator keys() {
        return IntIterator.wrap(this.layer.keys());
    }

    @Override
    public boolean containsKey(int node) {
        return this.layer.containsKey(node);
    }

    @Override
    public IntIterator get(int node) {
        return IntIterator.wrap(this.layer.getValues(node));
    }

    @Override
    public void addPath(int node, int parent) {
        this.layer.add(node, parent);
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
