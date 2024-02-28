package com.vortex.vortexdb.backend.store;

import com.vortex.common.config.VortexConfig;
import java.util.Iterator;
import java.util.Map;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;

public interface BackendStore {

    // Store name
    public String store();

    // Database name
    public String database();

    // Get the parent provider
    public BackendStoreProvider provider();

    // Whether it is the storage of schema
    public boolean isSchemaStore();

    // Open/close database
    public void open(VortexConfig config);
    public void close();
    public boolean opened();

    // Initialize/clear database
    public void init();
    public void clear(boolean clearSpace);
    public boolean initialized();

    // Delete all data of database (keep table structure)
    public void truncate();

    // Add/delete data
    public void mutate(BackendMutation mutation);

    // Query data
    public Iterator<BackendEntry> query(Query query);
    public Number queryNumber(Query query);

    // Transaction
    public void beginTx();
    public void commitTx();
    public void rollbackTx();

    // Get metadata by key
    public <R> R metadata(VortexType type, String meta, Object[] args);

    // Backend features
    public BackendFeatures features();

    // Generate an id for a specific type
    public default Id nextId(VortexType type) {
        final int MAX_TIMES = 1000;
        // Do get-increase-get-compare operation
        long counter = 0L;
        long expect = -1L;
        synchronized(this) {
            for (int i = 0; i < MAX_TIMES; i++) {
                counter = this.getCounter(type);

                if (counter == expect) {
                    break;
                }
                // Increase local counter
                expect = counter + 1L;
                // Increase remote counter
                this.increaseCounter(type, 1L);
            }
        }

        E.checkState(counter != 0L, "Please check whether '%s' is OK",
                this.provider().type());
        E.checkState(counter == expect, "'%s' is busy please try again",
                this.provider().type());
        return IdGenerator.of(expect);
    }

    // Set next id >= lowest for a specific type
    public default void setCounterLowest(VortexType type, long lowest) {
        long current = this.getCounter(type);
        if (current >= lowest) {
            return;
        }
        long increment = lowest - current;
        this.increaseCounter(type, increment);
    }

    public default String olapTableName(VortexType type) {
        StringBuilder sb = new StringBuilder(7);
        sb.append(this.store())
                .append("_")
                .append(VortexType.OLAP.string())
                .append("_")
                .append(type.string());
        return sb.toString().toLowerCase();
    }

    public default String olapTableName(Id id) {
        StringBuilder sb = new StringBuilder(5 + 4);
        sb.append(this.store())
                .append("_")
                .append(VortexType.OLAP.string())
                .append("_")
                .append(id.asLong());
        return sb.toString().toLowerCase();
    }

    // Increase next id for specific type
    public void increaseCounter(VortexType type, long increment);

    // Get current counter for a specific type
    public long getCounter(VortexType type);

    public default void createOlapTable(Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStore.createOlapTable()");
    }

    public default void checkAndRegisterOlapTable(Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStore.checkAndRegisterOlapTable()");
    }

    public default void clearOlapTable(Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStore.clearOlapTable()");
    }

    public default void removeOlapTable(Id pkId) {
        throw new UnsupportedOperationException(
                "BackendStore.removeOlapTable()");
    }

    public default Map<String, String> createSnapshot(String snapshotDir) {
        throw new UnsupportedOperationException("createSnapshot");
    }

    public default void resumeSnapshot(String snapshotDir,
                                       boolean deleteSnapshot) {
        throw new UnsupportedOperationException("resumeSnapshot");
    }

    static enum TxState {
        BEGIN, COMMITTING, COMMITT_FAIL, ROLLBACKING, ROLLBACK_FAIL, CLEAN
    }
}
