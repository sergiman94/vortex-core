
package com.vortex.vortexdb.job.schema;

import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.schema.PropertyKey;

public class OlapPropertyKeyCreateJob extends SchemaJob {

    @Override
    public String type() {
        return CREATE_OLAP;
    }

    @Override
    public Object execute() {
        SchemaTransaction schemaTx = this.params().schemaTransaction();
        PropertyKey propertyKey = schemaTx.getPropertyKey(this.schemaId());
        // Create olap index label schema
        schemaTx.createIndexLabelForOlapPk(propertyKey);
        // Create olap data table
        this.params().graphTransaction().createOlapPk(propertyKey.id());
        return null;
    }
}
