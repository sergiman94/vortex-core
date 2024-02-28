package com.vortex.backend.store.cassandra;

import com.vortex.vortexdb.backend.store.BackendFeatures;

public class CassandraFeatures implements BackendFeatures {

    @Override
    public boolean supportsScanToken() {
        return true;
    }

    @Override
    public boolean supportsScanKeyPrefix() {
        return false;
    }

    @Override
    public boolean supportsScanKeyRange() {
        return false;
    }

    @Override
    public boolean supportsQuerySchemaByName() {
        // Cassandra support secondary index
        return true;
    }

    @Override
    public boolean supportsQueryByLabel() {
        // Cassandra support secondary index
        return true;
    }

    @Override
    public boolean supportsQueryWithInCondition() {
        return true;
    }

    @Override
    public boolean supportsQueryWithRangeCondition() {
        return true;
    }

    @Override
    public boolean supportsQueryWithOrderBy() {
        return true;
    }

    @Override
    public boolean supportsQueryWithContains() {
        return true;
    }

    @Override
    public boolean supportsQueryWithContainsKey() {
        return true;
    }

    @Override
    public boolean supportsQueryByPage() {
        return true;
    }

    @Override
    public boolean supportsQuerySortByInputIds() {
        return false;
    }

    @Override
    public boolean supportsDeleteEdgeByLabel() {
        return true;
    }

    @Override
    public boolean supportsUpdateVertexProperty() {
        return true;
    }

    @Override
    public boolean supportsMergeVertexProperty() {
        return false;
    }

    @Override
    public boolean supportsUpdateEdgeProperty() {
        return true;
    }

    @Override
    public boolean supportsTransaction() {
        // Cassandra support tx(atomicity level) with batch API
        // https://docs.datastax.com/en/cassandra/2.1/cassandra/dml/dml_atomicity_c.html
        return true;
    }

    @Override
    public boolean supportsNumberType() {
        return true;
    }

    @Override
    public boolean supportsAggregateProperty() {
        return false;
    }

    @Override
    public boolean supportsTtl() {
        return true;
    }

    @Override
    public boolean supportsOlapProperties() {
        return true;
    }
}
