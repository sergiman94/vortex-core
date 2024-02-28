
package com.vortex.vortexdb.traversal.algorithm.records;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.common.perf.PerfUtil.Watched;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser.Path;
import com.vortex.vortexdb.traversal.algorithm.VortexTraverser.PathSet;
import com.vortex.vortexdb.traversal.algorithm.records.record.Int2IntRecord;
import com.vortex.vortexdb.traversal.algorithm.records.record.Record;
import com.vortex.vortexdb.traversal.algorithm.records.record.RecordType;
import com.vortex.vortexdb.type.define.CollectionType;
import com.vortex.vortexdb.util.collection.CollectionFactory;
import com.vortex.vortexdb.util.collection.IntIterator;
import com.vortex.vortexdb.util.collection.IntIterator.MapperInt2ObjectIterator;
import com.vortex.vortexdb.util.collection.IntMap;
import com.vortex.vortexdb.util.collection.IntSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;

public abstract class SingleWayMultiPathsRecords extends AbstractRecords {

    private final Stack<Record> records;

    private final int sourceCode;
    private final boolean nearest;
    private final IntSet accessedVertices;

    private IntIterator parentRecordKeys;

    public SingleWayMultiPathsRecords(RecordType type, boolean concurrent,
                                      Id source, boolean nearest) {
        super(type, concurrent);

        this.nearest = nearest;

        this.sourceCode = this.code(source);
        Record firstRecord = this.newRecord();
        firstRecord.addPath(this.sourceCode, 0);
        this.records = new Stack<>();
        this.records.push(firstRecord);

        this.accessedVertices = CollectionFactory.newIntSet();
    }

    @Override
    public void startOneLayer(boolean forward) {
        Record parentRecord = this.records.peek();
        this.currentRecord(this.newRecord(), parentRecord);
        this.parentRecordKeys = parentRecord.keys();
    }

    @Override
    public void finishOneLayer() {
        this.records.push(this.currentRecord());
    }

    @Override
    public boolean hasNextKey() {
        return this.parentRecordKeys.hasNext();
    }

    @Override
    public Id nextKey() {
        return this.id(this.parentRecordKeys.next());
    }

    @Override
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        PathSet paths = new PathSet();
        for (int i = 1; i < this.records.size(); i++) {
            IntIterator iterator = this.records.get(i).keys();
            while (iterator.hasNext()) {
                paths.add(this.linkPath(i, iterator.next()));
            }
        }
        return paths;
    }

    @Override
    public long accessed() {
        return this.accessedVertices.size();
    }

    public Iterator<Id> keys() {
        return new MapperInt2ObjectIterator<>(this.parentRecordKeys, this::id);
    }

    @Watched
    public void addPath(Id source, Id target) {
        int sourceCode = this.code(source);
        int targetCode = this.code(target);
        if (this.nearest && this.accessedVertices.contains(targetCode) ||
            !this.nearest && this.currentRecord().containsKey(targetCode) ||
            targetCode == this.sourceCode) {
            return;
        }
        this.currentRecord().addPath(targetCode, sourceCode);

        this.accessedVertices.add(targetCode);
    }

    protected final Path linkPath(int target) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        // Find the layer where the target is located
        int foundLayer = -1;
        for (int i = 0; i < this.records.size(); i++) {
            IntMap layer = this.layer(i);
            if (!layer.containsKey(target)) {
                continue;
            }

            foundLayer = i;
            // Collect self node
            ids.add(this.id(target));
            break;
        }
        // If a layer found, then concat parents
        if (foundLayer > 0) {
            for (int i = foundLayer; i > 0; i--) {
                IntMap layer = this.layer(i);
                // Uptrack parents
                target = layer.get(target);
                ids.add(this.id(target));
            }
        }
        return new Path(ids);
    }

    protected final Path linkPath(int layerIndex, int target) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        IntMap layer = this.layer(layerIndex);
        if (!layer.containsKey(target)) {
            throw new VortexException("Failed to get path for %s",
                                    this.id(target));
        }
        // Collect self node
        ids.add(this.id(target));
        // Concat parents
        for (int i = layerIndex; i > 0; i--) {
            layer = this.layer(i);
            // Uptrack parents
            target = layer.get(target);
            ids.add(this.id(target));
        }
        Collections.reverse(ids);
        return new Path(ids);
    }

    protected final IntMap layer(int layerIndex) {
        Record record = this.records.elementAt(layerIndex);
        return ((Int2IntRecord) record).layer();
    }

    protected final Stack<Record> records() {
        return this.records;
    }

    public abstract int size();

    public abstract List<Id> ids(long limit);

    public abstract PathSet paths(long limit);
}
