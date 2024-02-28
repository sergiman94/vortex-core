package com.vortex.backend.store.cassandra;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.serializer.TableBackendEntry;
import com.vortex.vortexdb.type.VortexType;

public class CassandraBackendEntry extends TableBackendEntry {

    public CassandraBackendEntry(Id id) {
        super(id);
    }

    public CassandraBackendEntry(VortexType type) {
        this(type, null);
    }

    public CassandraBackendEntry(VortexType type, Id id) {
        this(new Row(type, id));
    }

    public CassandraBackendEntry(TableBackendEntry.Row row) {
        super(row);
    }
}
