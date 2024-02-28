
package com.vortex.vortexdb.traversal.algorithm.records;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser.PathSet;

import java.util.function.Function;

public interface Records {

    public void startOneLayer(boolean forward);

    public void finishOneLayer();

    public boolean hasNextKey();

    public Id nextKey();

    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring);

    public long accessed();
}
