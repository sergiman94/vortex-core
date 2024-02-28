
package com.vortex.vortexdb.job.schema;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.schema.PropertyKey;

public class OlapPropertyKeyRemoveJob extends OlapPropertyKeyClearJob {

    @Override
    public String type() {
        return REMOVE_OLAP;
    }

    @Override
    public Object execute() {
        Id olap = this.schemaId();

        // Remove olap data table
        this.params().graphTransaction().removeOlapPk(olap);

        // Remove corresponding index label and index data
        Id indexLabel = findOlapIndexLabel(this.params(), olap);
        if (indexLabel != null) {
            removeIndexLabel(this.params(), indexLabel);
        }

        // Remove olap property key
        SchemaTransaction schemaTx = this.params().schemaTransaction();
        PropertyKey propertyKey = schemaTx.getPropertyKey(olap);
        removeSchema(schemaTx, propertyKey);
        return null;
    }
}
