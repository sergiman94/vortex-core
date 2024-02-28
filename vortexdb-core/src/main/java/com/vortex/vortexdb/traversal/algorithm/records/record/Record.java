
package com.vortex.vortexdb.traversal.algorithm.records.record;

import com.vortex.vortexdb.util.collection.IntIterator;

public interface Record {

    public IntIterator keys();

    public boolean containsKey(int node);

    public IntIterator get(int node);

    public void addPath(int node, int parent);

    public int size();

    public boolean concurrent();
}
