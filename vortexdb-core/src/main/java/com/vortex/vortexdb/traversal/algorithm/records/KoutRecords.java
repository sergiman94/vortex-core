
package com.vortex.vortexdb.traversal.algorithm.records;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser.PathSet;
import com.vortex.vortexdb.traversal.algorithm.records.record.Record;
import com.vortex.vortexdb.traversal.algorithm.records.record.RecordType;
import com.vortex.vortexdb.type.define.CollectionType;
import com.vortex.vortexdb.util.collection.CollectionFactory;
import com.vortex.vortexdb.util.collection.IntIterator;

import java.util.List;
import java.util.Stack;

import static com.vortex.vortexdb.backend.query.Query.NO_LIMIT;

public class KoutRecords extends SingleWayMultiPathsRecords {

    public KoutRecords(boolean concurrent, Id source, boolean nearest) {
        super(RecordType.INT, concurrent, source, nearest);
    }

    @Override
    public int size() {
        return this.currentRecord().size();
    }

    @Override
    public List<Id> ids(long limit) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        IntIterator iterator = this.records().peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            ids.add(this.id(iterator.next()));
        }
        return ids;
    }

    @Override
    public PathSet paths(long limit) {
        PathSet paths = new PathSet();
        Stack<Record> records = this.records();
        IntIterator iterator = records.peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            paths.add(this.linkPath(records.size() - 1, iterator.next()));
        }
        return paths;
    }
}
