
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

public class KneighborRecords extends SingleWayMultiPathsRecords {

    public KneighborRecords(boolean concurrent,
                            Id source, boolean nearest) {
        super(RecordType.INT, concurrent, source, nearest);
    }

    @Override
    public int size() {
        return (int) this.accessed();
    }

    @Override
    public List<Id> ids(long limit) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        Stack<Record> records = this.records();
        // Not include record(i=0) to ignore source vertex
        for (int i = 1; i < records.size(); i++) {
            IntIterator iterator = records.get(i).keys();
            while ((limit == NO_LIMIT || limit > 0L) && iterator.hasNext()) {
                ids.add(this.id(iterator.next()));
                limit--;
            }
        }
        return ids;
    }

    @Override
    public PathSet paths(long limit) {
        PathSet paths = new PathSet();
        Stack<Record> records = this.records();
        for (int i = 1; i < records.size(); i++) {
            IntIterator iterator = records.get(i).keys();
            while ((limit == NO_LIMIT || limit > 0L) && iterator.hasNext()) {
                paths.add(this.linkPath(i, iterator.next()));
                limit--;
            }
        }
        return paths;
    }
}
