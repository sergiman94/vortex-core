
package com.vortex.vortexdb.traversal.algorithm.records;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.common.perf.PerfUtil.Watched;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser.PathSet;
import com.vortex.vortexdb.traversal.algorithm.records.record.RecordType;

import java.util.function.Function;

public class PathsRecords extends DoubleWayMultiPathsRecords {

    public PathsRecords(boolean concurrent, Id sourceV, Id targetV) {
        super(RecordType.ARRAY, concurrent, sourceV, targetV);
    }

    @Watched
    @Override
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        assert all;
        int targetCode = this.code(target);
        int parentCode = this.current();
        PathSet paths = PathSet.EMPTY;

        // Traverse backtrace is not allowed, stop now
        if (this.parentsContain(targetCode)) {
            return paths;
        }

        // Add to current layer
        this.addPath(targetCode, parentCode);
        // If cross point exists, path found, concat them
        if (this.movingForward() && this.targetContains(targetCode)) {
            paths = this.linkPath(parentCode, targetCode, ring);
        }
        if (!this.movingForward() && this.sourceContains(targetCode)) {
            paths = this.linkPath(targetCode, parentCode, ring);
        }
        return paths;
    }
}
