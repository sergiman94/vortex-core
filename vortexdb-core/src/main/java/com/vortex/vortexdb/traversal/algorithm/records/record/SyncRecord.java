
package com.vortex.vortexdb.traversal.algorithm.records.record;

import com.vortex.vortexdb.util.collection.IntIterator;

public class SyncRecord implements Record {

    private final Object lock;
    private final Record record;

    public SyncRecord(Record record) {
        this(record, null);
    }

    public SyncRecord(Record record, Object newLock) {
        if (record == null) {
            throw new IllegalArgumentException(
                      "Cannot create a SyncRecord on a null record");
        } else {
            this.record = record;
            this.lock = newLock == null ? this : newLock;
        }
    }

    @Override
    public IntIterator keys() {
        /*
         * Another threads call addPath() will change IntIterator inner array,
         * but in kout/kneighbor scenario it's ok because keys() and addPath()
         * won't be called simultaneously on same Record.
         */
        synchronized (this.lock) {
            return this.record.keys();
        }
    }

    @Override
    public boolean containsKey(int key) {
        synchronized (this.lock) {
            return this.record.containsKey(key);
        }
    }

    @Override
    public IntIterator get(int key) {
        synchronized (this.lock) {
            return this.record.get(key);
        }
    }

    @Override
    public void addPath(int node, int parent) {
        synchronized (this.lock) {
            this.record.addPath(node, parent);
        }
    }

    @Override
    public int size() {
        synchronized (this.lock) {
            return this.record.size();
        }
    }

    @Override
    public boolean concurrent() {
        return true;
    }
}
