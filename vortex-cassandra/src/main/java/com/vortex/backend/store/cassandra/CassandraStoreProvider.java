package com.vortex.backend.store.cassandra;

import com.vortex.vortexdb.backend.store.AbstractBackendStoreProvider;
import com.vortex.vortexdb.backend.store.BackendStore;
import com.vortex.backend.store.cassandra.CassandraStore.CassandraGraphStore;
import com.vortex.backend.store.cassandra.CassandraStore.CassandraSchemaStore;

public class CassandraStoreProvider extends AbstractBackendStoreProvider {

    protected String keyspace() {
        return this.graph().toLowerCase();
    }

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new CassandraSchemaStore(this, this.keyspace(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new CassandraGraphStore(this, this.keyspace(), store);
    }

    @Override
    public String type() {
        return "cassandra";
    }

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports backend table version checking
         * [1.1] HugeGraph-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         * [1.3] #455: fix scylladb backend doesn't support label query in page
         * [1.4] #270 & #398: support shard-index and vertex + sortkey prefix,
         *                    also split range table to rangeInt, rangeFloat,
         *                    rangeLong and rangeDouble
         * [1.5] #633: support unique index
         * [1.6] #661 & #680: support bin serialization for cassandra
         * [1.7] #691: support aggregate property
         * [1.8] #746: support userdata for indexlabel
         * [1.9] #295: support ttl for vertex and edge
         * [1.10] #1333: support read frequency for property key
         * [1.11] #1506: support olap properties
         */
        return "1.11";
    }
}
