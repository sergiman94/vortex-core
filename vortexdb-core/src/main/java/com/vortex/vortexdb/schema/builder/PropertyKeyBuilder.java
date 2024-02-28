
package com.vortex.vortexdb.schema.builder;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.vortexdb.exception.ExistedException;
import com.vortex.vortexdb.exception.NotAllowException;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.vortexdb.exception.NotSupportException;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.schema.Userdata;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.*;
import com.vortex.common.util.E;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public class PropertyKeyBuilder extends AbstractBuilder
                                implements PropertyKey.Builder {

    private Id id;
    private String name;
    private DataType dataType;
    private Cardinality cardinality;
    private AggregateType aggregateType;
    private WriteType writeType;
    private boolean checkExist;
    private Userdata userdata;

    public PropertyKeyBuilder(SchemaTransaction transaction,
                              Vortex graph, String name) {
        super(transaction, graph);
        E.checkNotNull(name, "name");
        this.id = null;
        this.name = name;
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
        this.aggregateType = AggregateType.NONE;
        this.writeType = WriteType.OLTP;
        this.userdata = new Userdata();
        this.checkExist = true;
    }

    public PropertyKeyBuilder(SchemaTransaction transaction,
                              Vortex graph, PropertyKey copy) {
        super(transaction, graph);
        E.checkNotNull(copy, "copy");
        this.id = null;
        this.name = copy.name();
        this.dataType = copy.dataType();
        this.cardinality = copy.cardinality();
        this.aggregateType = copy.aggregateType();
        this.writeType = copy.writeType();
        this.userdata = new Userdata(copy.userdata());
        this.checkExist = false;
    }

    @Override
    public PropertyKey build() {
        Id id = this.validOrGenerateId(VortexType.PROPERTY_KEY,
                                       this.id, this.name);
        PropertyKey propertyKey = new PropertyKey(this.graph(), id, this.name);
        propertyKey.dataType(this.dataType);
        propertyKey.cardinality(this.cardinality);
        propertyKey.aggregateType(this.aggregateType);
        propertyKey.writeType(this.writeType);
        propertyKey.userdata(this.userdata);
        return propertyKey;
    }

    /**
     * Check whether this has same properties with propertyKey.
     * Only dataType, cardinality, aggregateType are checked.
     * The id, checkExist, userdata are not checked.
     * @param propertyKey to be compared with
     * @return true if this has same properties with propertyKey
     */
    private boolean hasSameProperties(PropertyKey propertyKey) {
        // dataType is enum
        if (this.dataType != propertyKey.dataType()) {
            return false;
        }

        // cardinality is enum
        if (this.cardinality != propertyKey.cardinality()) {
            return false;
        }

        // aggregateType is enum
        if (this.aggregateType != propertyKey.aggregateType()) {
            return false;
        }

        if (this.writeType != propertyKey.writeType()) {
            return false;
        }

        // all properties are same, return true.
        return true;
    }

    @Override
    public SchemaElement.TaskWithSchema createWithTask() {
        VortexType type = VortexType.PROPERTY_KEY;
        this.checkSchemaName(this.name);

        return this.lockCheckAndCreateSchema(type, this.name, name -> {
            PropertyKey propertyKey = this.propertyKeyOrNull(name);
            if (propertyKey != null) {
                if (this.checkExist || !hasSameProperties(propertyKey)) {
                    throw new ExistedException(type, name);
                }
                return new SchemaElement.TaskWithSchema(propertyKey,
                                                        IdGenerator.ZERO);
            }
            this.checkSchemaIdIfRestoringMode(type, this.id);

            Userdata.check(this.userdata, Action.INSERT);
            this.checkAggregateType();
            this.checkOlap();

            propertyKey = this.build();
            assert propertyKey.name().equals(name);
            Id id = this.graph().addPropertyKey(propertyKey);
            return new SchemaElement.TaskWithSchema(propertyKey, id);
        });
    }

    @Override
    public PropertyKey create() {
        // Create index label async
        SchemaElement.TaskWithSchema propertyKeyWithTask =
                                     this.createWithTask();

        Id task = propertyKeyWithTask.task();
        if (task == IdGenerator.ZERO) {
            /*
             * Task id will be IdGenerator.ZERO if creating property key
             * already exists or creating property key is oltp
             */
            return propertyKeyWithTask.propertyKey();
        }

        // Wait task completed (change to sync mode)
        Vortex graph = this.graph();
        long timeout = graph.option(CoreOptions.TASK_WAIT_TIMEOUT);
        try {
            graph.taskScheduler().waitUntilTaskCompleted(task, timeout);
        } catch (TimeoutException e) {
            throw new VortexException(
                      "Failed to wait property key create task completed", e);
        }

        // Return property key without task-info
        return propertyKeyWithTask.propertyKey();
    }

    @Override
    public PropertyKey append() {
        PropertyKey propertyKey = this.propertyKeyOrNull(this.name);
        if (propertyKey == null) {
            throw new NotFoundException("Can't update property key '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        Userdata.check(this.userdata, Action.APPEND);

        propertyKey.userdata(this.userdata);
        this.graph().addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public PropertyKey eliminate() {
        PropertyKey propertyKey = this.propertyKeyOrNull(this.name);
        if (propertyKey == null) {
            throw new NotFoundException("Can't update property key '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        Userdata.check(this.userdata, Action.ELIMINATE);

        propertyKey.removeUserdata(this.userdata);
        this.graph().addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public Id remove() {
        PropertyKey propertyKey = this.propertyKeyOrNull(this.name);
        if (propertyKey == null) {
            return IdGenerator.ZERO;
        }
        return this.graph().removePropertyKey(propertyKey.id());
    }

    @Override
    public PropertyKeyBuilder id(long id) {
        E.checkArgument(id != 0L,
                        "Not allowed to assign 0 as property key id");
        this.id = IdGenerator.of(id);
        return this;
    }

    @Override
    public PropertyKeyBuilder asText() {
        this.dataType = DataType.TEXT;
        return this;
    }

    @Override
    public PropertyKeyBuilder asInt() {
        this.dataType = DataType.INT;
        return this;
    }

    @Override
    public PropertyKeyBuilder asDate() {
        this.dataType = DataType.DATE;
        return this;
    }

    @Override
    public PropertyKeyBuilder asUUID() {
        this.dataType = DataType.UUID;
        return this;
    }

    @Override
    public PropertyKeyBuilder asBoolean() {
        this.dataType = DataType.BOOLEAN;
        return this;
    }

    @Override
    public PropertyKeyBuilder asByte() {
        this.dataType = DataType.BYTE;
        return this;
    }

    @Override
    public PropertyKeyBuilder asBlob() {
        this.dataType = DataType.BLOB;
        return this;
    }

    @Override
    public PropertyKeyBuilder asDouble() {
        this.dataType = DataType.DOUBLE;
        return this;
    }

    @Override
    public PropertyKeyBuilder asFloat() {
        this.dataType = DataType.FLOAT;
        return this;
    }

    @Override
    public PropertyKeyBuilder asLong() {
        this.dataType = DataType.LONG;
        return this;
    }

    @Override
    public PropertyKeyBuilder valueSingle() {
        this.cardinality = Cardinality.SINGLE;
        return this;
    }

    @Override
    public PropertyKeyBuilder valueList() {
        this.cardinality = Cardinality.LIST;
        return this;
    }

    @Override
    public PropertyKeyBuilder valueSet() {
        this.cardinality = Cardinality.SET;
        return this;
    }

    @Override
    public PropertyKeyBuilder calcMax() {
        this.aggregateType = AggregateType.MAX;
        return this;
    }

    @Override
    public PropertyKeyBuilder calcMin() {
        this.aggregateType = AggregateType.MIN;
        return this;
    }

    @Override
    public PropertyKeyBuilder calcSum() {
        this.aggregateType = AggregateType.SUM;
        return this;
    }

    @Override
    public PropertyKeyBuilder calcOld() {
        this.aggregateType = AggregateType.OLD;
        return this;
    }

    @Override
    public PropertyKey.Builder calcSet() {
        this.aggregateType = AggregateType.SET;
        return this;
    }

    @Override
    public PropertyKey.Builder calcList() {
        this.aggregateType = AggregateType.LIST;
        return this;
    }

    @Override
    public PropertyKey.Builder writeType(WriteType writeType) {
        this.writeType = writeType;
        return this;
    }

    @Override
    public PropertyKeyBuilder userdata(String key, Object value) {
        this.userdata.put(key, value);
        return this;
    }

    @Override
    public PropertyKeyBuilder cardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
        return this;
    }

    @Override
    public PropertyKeyBuilder dataType(DataType dataType) {
        this.dataType = dataType;
        return this;
    }

    @Override
    public PropertyKey.Builder aggregateType(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
        return this;
    }

    @Override
    public PropertyKeyBuilder userdata(Map<String, Object> userdata) {
        this.userdata.putAll(userdata);
        return this;
    }

    @Override
    public PropertyKeyBuilder ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public PropertyKeyBuilder checkExist(boolean checkExist) {
        this.checkExist = checkExist;
        return this;
    }

    private void checkStableVars() {
        if (this.dataType != DataType.TEXT) {
            throw new NotAllowException("Not allowed to update data type " +
                                        "for property key '%s'", this.name);
        }
        if (this.cardinality != Cardinality.SINGLE) {
            throw new NotAllowException("Not allowed to update cardinality " +
                                        "for property key '%s'", this.name);
        }
    }

    private void checkAggregateType() {
        if (this.aggregateType.isNone()) {
            return;
        }

        if (this.aggregateType.isSet() &&
            this.cardinality == Cardinality.SET ||
            this.aggregateType.isList() &&
            this.cardinality == Cardinality.LIST) {
            return;
        }

        if (this.cardinality != Cardinality.SINGLE ||
            this.aggregateType.isUnion()) {
            throw new NotAllowException("Not allowed to set aggregate type " +
                                        "'%s' for property key '%s' with " +
                                        "cardinality '%s'",
                                        this.aggregateType, this.name,
                                        this.cardinality);
        }

        if (this.aggregateType.isSum() && this.dataType.isDate()) {
            throw new NotAllowException(
                      "Not allowed to set aggregate type '%s' for " +
                      "property key '%s' with data type '%s'",
                      this.aggregateType, this.name, this.dataType);
        }


        if (this.aggregateType.isNumber() &&
            !this.dataType.isNumber() && !this.dataType.isDate()) {
            throw new NotAllowException(
                      "Not allowed to set aggregate type '%s' for " +
                      "property key '%s' with data type '%s'",
                      this.aggregateType, this.name, this.dataType);
        }
    }

    private void checkOlap() {
        if (this.writeType == WriteType.OLTP) {
            return;
        }

        if (!this.graph().backendStoreFeatures().supportsOlapProperties()) {
            throw new NotSupportException(
                      "olap property key '%s' for backend '%s'",
                      this.name, this.graph().backend());
        }

        if (!this.aggregateType.isNone()) {
            throw new NotAllowException(
                      "Not allowed to set aggregate type '%s' for olap " +
                      "property key '%s'", this.aggregateType, this.name);
        }

        if (this.writeType == WriteType.OLAP_RANGE &&
            !this.dataType.isNumber() && !this.dataType.isDate()) {
            throw new NotAllowException(
                      "Not allowed to set write type to OLAP_RANGE for " +
                      "property key '%s' with data type '%s'",
                      this.name, this.dataType);
        }
    }
}
