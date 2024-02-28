
package com.vortex.vortexdb.traversal.algorithm.records.record;

public class RecordFactory {

    public static Record newRecord(RecordType type) {
        return newRecord(type, false);
    }

    public static Record newRecord(RecordType type, boolean concurrent) {
        Record record;
        switch (type) {
            case INT:
                record = new Int2IntRecord();
                break;
            case SET:
                record = new Int2SetRecord();
                break;
            case ARRAY:
                record = new Int2ArrayRecord();
                break;
            default:
                throw new AssertionError("Unsupported record type: " + type);
        }

        if (concurrent && !record.concurrent()) {
            record = new SyncRecord(record);
        }

        return record;
    }
}
