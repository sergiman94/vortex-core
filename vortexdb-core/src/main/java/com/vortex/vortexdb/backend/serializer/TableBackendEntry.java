
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.Cardinality;
import com.vortex.vortexdb.type.define.VortexKeys;
import org.apache.commons.lang3.NotImplementedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TableBackendEntry implements BackendEntry {

    public static class Row {

        private VortexType type;
        private Id id;
        private Id subId;
        private Map<VortexKeys, Object> columns;
        private long ttl;

        public Row(VortexType type) {
            this(type, null);
        }

        public Row(VortexType type, Id id) {
            this.type = type;
            this.id = id;
            this.subId = null;
            this.columns = new ConcurrentHashMap<>();
            this.ttl = 0L;
        }

        public VortexType type() {
            return this.type;
        }

        public Id id() {
            return this.id;
        }

        public Map<VortexKeys, Object> columns() {
            return this.columns;
        }

        @SuppressWarnings("unchecked")
        public <T> T column(VortexKeys key) {
            // The T must be primitive type, or list/set/map of primitive type
            return (T) this.columns.get(key);
        }

        public <T> void column(VortexKeys key, T value) {
            this.columns.put(key, value);
        }

        public <T> void column(VortexKeys key, T value, Cardinality c) {
            switch (c) {
                case SINGLE:
                    this.column(key, value);
                    break;
                case SET:
                    // Avoid creating new Set when the key exists
                    if (!this.columns.containsKey(key)) {
                        this.columns.putIfAbsent(key, new LinkedHashSet<>());
                    }
                    this.<Set<T>>column(key).add(value);
                    break;
                case LIST:
                    // Avoid creating new List when the key exists
                    if (!this.columns.containsKey(key)) {
                        this.columns.putIfAbsent(key, new LinkedList<>());
                    }
                    this.<List<T>>column(key).add(value);
                    break;
                default:
                    throw new AssertionError("Unsupported cardinality: " + c);
            }
        }

        public <T> void column(VortexKeys key, Object name, T value) {
            if (!this.columns.containsKey(key)) {
                this.columns.putIfAbsent(key, new ConcurrentHashMap<>());
            }
            this.<Map<Object, T>>column(key).put(name, value);
        }

        public void ttl(long ttl) {
            this.ttl = ttl;
        }

        public long ttl() {
            return this.ttl;
        }

        @Override
        public String toString() {
            return String.format("Row{type=%s, id=%s, columns=%s}",
                                 this.type, this.id, this.columns);
        }
    }

    private final Row row;
    private final List<Row> subRows;

    // NOTE: selfChanged is false when the row has not changed but subRows has.
    private boolean selfChanged = true;
    private boolean olap = false;

    public TableBackendEntry(Id id) {
        this(null, id);
    }

    public TableBackendEntry(VortexType type) {
        this(type, null);
    }

    public TableBackendEntry(VortexType type, Id id) {
        this(new Row(type, id));
    }

    public TableBackendEntry(Row row) {
        this.row = row;
        this.subRows = new ArrayList<>();
        this.selfChanged = true;
    }

    @Override
    public VortexType type() {
        return this.row.type;
    }

    public void type(VortexType type) {
        this.row.type = type;
    }

    @Override
    public Id id() {
        return this.row.id;
    }

    @Override
    public Id originId() {
        return this.row.id;
    }

    public void id(Id id) {
        this.row.id = id;
    }

    @Override
    public Id subId() {
        return this.row.subId;
    }

    public void subId(Id subId) {
        this.row.subId = subId;
    }

    public void selfChanged(boolean changed) {
        this.selfChanged = changed;
    }

    public boolean selfChanged() {
        return this.selfChanged;
    }

    public void olap(boolean olap) {
        this.olap = olap;
    }

    public boolean olap() {
        return this.olap;
    }

    public Row row() {
        return this.row;
    }

    public Map<VortexKeys, Object> columnsMap() {
        return this.row.columns();
    }

    public <T> void column(VortexKeys key, T value) {
        this.row.column(key, value);
    }

    public <T> void column(VortexKeys key, Object name, T value) {
        this.row.column(key, name, value);
    }

    public <T> void column(VortexKeys key, T value, Cardinality c) {
        this.row.column(key, value, c);
    }

    public <T> T column(VortexKeys key) {
        return this.row.column(key);
    }

    public void subRow(Row row) {
        this.subRows.add(row);
    }

    public List<Row> subRows() {
        return this.subRows;
    }

    public void ttl(long ttl) {
        this.row.ttl(ttl);
    }

    @Override
    public long ttl() {
        return this.row.ttl();
    }

    @Override
    public String toString() {
        return String.format("TableBackendEntry{%s, sub-rows: %s}",
                             this.row.toString(),
                             this.subRows.toString());
    }

    @Override
    public int columnsSize() {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public Collection<BackendEntry.BackendColumn> columns() {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public void columns(Collection<BackendEntry.BackendColumn> bytesColumns) {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public void columns(BackendEntry.BackendColumn... bytesColumns) {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public void merge(BackendEntry other) {
        throw new NotImplementedException("Not supported by table backend");
    }

    @Override
    public boolean mergable(BackendEntry other) {
        if (!(other instanceof TableBackendEntry)) {
            return false;
        }
        TableBackendEntry tableEntry = (TableBackendEntry) other;
        Object selfId = this.column(VortexKeys.ID);
        Object otherId = tableEntry.column(VortexKeys.ID);
        if (!selfId.equals(otherId)) {
            return false;
        }
        Id key = tableEntry.subId();
        Object value = tableEntry.row().column(VortexKeys.PROPERTY_VALUE);
        this.row().column(VortexKeys.PROPERTIES, key.asLong(), value);
        return true;
    }

    @Override
    public void clear() {
        throw new NotImplementedException("Not supported by table backend");
    }
}
