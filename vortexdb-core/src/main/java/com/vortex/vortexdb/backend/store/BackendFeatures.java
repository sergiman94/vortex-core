

package com.vortex.vortexdb.backend.store;

public interface BackendFeatures {

    public default boolean supportsPersistence() {
        return true;
    }

    public default boolean supportsSharedStorage() {
        return true;
    }

    public default boolean supportsSnapshot() {
        return false;
    }

    public boolean supportsScanToken();

    public boolean supportsScanKeyPrefix();

    public boolean supportsScanKeyRange();

    public boolean supportsQuerySchemaByName();

    public boolean supportsQueryByLabel();

    public boolean supportsQueryWithInCondition();

    public boolean supportsQueryWithRangeCondition();

    public boolean supportsQueryWithContains();

    public boolean supportsQueryWithContainsKey();

    public boolean supportsQueryWithOrderBy();

    public boolean supportsQueryByPage();

    public boolean supportsQuerySortByInputIds();

    public boolean supportsDeleteEdgeByLabel();

    public boolean supportsUpdateVertexProperty();

    public boolean supportsMergeVertexProperty();

    public boolean supportsUpdateEdgeProperty();

    public boolean supportsTransaction();

    public boolean supportsNumberType();

    public boolean supportsAggregateProperty();

    public boolean supportsTtl();

    public boolean supportsOlapProperties();
}
