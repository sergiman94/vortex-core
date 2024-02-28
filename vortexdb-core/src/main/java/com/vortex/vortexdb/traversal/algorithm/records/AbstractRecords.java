
package com.vortex.vortexdb.traversal.algorithm.records;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.common.perf.PerfUtil.Watched;
import com.vortex.vortexdb.traversal.algorithm.records.record.Record;
import com.vortex.vortexdb.traversal.algorithm.records.record.RecordFactory;
import com.vortex.vortexdb.traversal.algorithm.records.record.RecordType;
import com.vortex.vortexdb.util.collection.ObjectIntMapping;
import com.vortex.vortexdb.util.collection.ObjectIntMappingFactory;

public abstract class AbstractRecords implements Records {

    private final ObjectIntMapping<Id> idMapping;
    private final RecordType type;
    private final boolean concurrent;
    private Record currentRecord;
    private Record parentRecord;

    public AbstractRecords(RecordType type, boolean concurrent) {
        this.type = type;
        this.concurrent = concurrent;
        this.parentRecord = null;
        this.idMapping = ObjectIntMappingFactory.newObjectIntMapping(this.concurrent);
    }

    @Watched
    protected final int code(Id id) {
        if (id.number()) {
            long l = id.asLong();
            if (0 <= l && l <= Integer.MAX_VALUE) {
                return (int) l;
            }
        }
        int code = this.idMapping.object2Code(id);
        assert code > 0;
        return -code;
    }

    @Watched
    protected final Id id(int code) {
        if (code >= 0) {
            return IdGenerator.of(code);
        }
        return this.idMapping.code2Object(-code);
    }

    protected final Record newRecord() {
        return RecordFactory.newRecord(this.type, this.concurrent);
    }

    protected final Record currentRecord() {
        return this.currentRecord;
    }

    protected void currentRecord(Record currentRecord, Record parentRecord) {
        this.parentRecord = parentRecord;
        this.currentRecord = currentRecord;
    }

    protected Record parentRecord() {
        return this.parentRecord;
    }
}
