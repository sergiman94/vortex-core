package com.vortex.backend.store.cassandra;

import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.id.IdUtil;
import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import com.vortex.vortexdb.backend.serializer.TableBackendEntry;
import com.vortex.vortexdb.backend.serializer.TableSerializer;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.structure.VortexIndex;
import com.vortex.vortexdb.structure.VortexProperty;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.DataType;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.util.E;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.vortexdb.util.JsonUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.nio.ByteBuffer;
import java.util.*;

public class CassandraSerializer extends TableSerializer {

    @Override
    public CassandraBackendEntry newBackendEntry(VortexType type, Id id) {
        return new CassandraBackendEntry(type, id);
    }

    @Override
    protected TableBackendEntry newBackendEntry(TableBackendEntry.Row row) {
        return new CassandraBackendEntry(row);
    }

    @Override
    protected TableBackendEntry newBackendEntry(VortexIndex index) {
        TableBackendEntry backendEntry = newBackendEntry(index.type(),
                                                         index.id());
        if (index.indexLabel().olap()) {
            backendEntry.olap(true);
        }
        return backendEntry;
    }

    @Override
    protected CassandraBackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof CassandraBackendEntry)) {
            throw new BackendException("Not supported by CassandraSerializer");
        }
        return (CassandraBackendEntry) backendEntry;
    }

    @Override
    protected Set<Object> parseIndexElemIds(TableBackendEntry entry) {
        return ImmutableSet.of(entry.column(VortexKeys.ELEMENT_IDS));
    }

    @Override
    protected Id toId(Number number) {
        return IdGenerator.of(number.longValue());
    }

    @Override
    protected Id[] toIdArray(Object object) {
        assert object instanceof Collection;
        @SuppressWarnings("unchecked")
        Collection<Number> numbers = (Collection<Number>) object;
        Id[] ids = new Id[numbers.size()];
        int i = 0;
        for (Number number : numbers) {
            ids[i++] = toId(number);
        }
        return ids;
    }

    @Override
    protected Object toLongSet(Collection<Id> ids) {
        Set<Long> results = InsertionOrderUtil.newSet();
        for (Id id : ids) {
            results.add(id.asLong());
        }
        return results;
    }

    @Override
    protected Object toLongList(Collection<Id> ids) {
        List<Long> results = new ArrayList<>(ids.size());
        for (Id id : ids) {
            results.add(id.asLong());
        }
        return results;
    }

    @Override
    protected void formatProperties(VortexElement element,
                                    TableBackendEntry.Row row) {
        if (!element.hasProperties() && !element.removed()) {
            row.column(VortexKeys.PROPERTIES, ImmutableMap.of());
        } else {
            // Format properties
            for (VortexProperty<?> prop : element.getProperties()) {
                this.formatProperty(prop, row);
            }
        }
    }

    @Override
    protected void parseProperties(VortexElement element,
                                   TableBackendEntry.Row row) {
        Map<Number, Object> props = row.column(VortexKeys.PROPERTIES);
        for (Map.Entry<Number, Object> prop : props.entrySet()) {
            Id pkeyId = this.toId(prop.getKey());
            this.parseProperty(pkeyId, prop.getValue(), element);
        }
    }

    @Override
    public BackendEntry writeOlapVertex(VortexVertex vertex) {
        CassandraBackendEntry entry = newBackendEntry(VortexType.OLAP,
                                                      vertex.id());
        entry.column(VortexKeys.ID, this.writeId(vertex.id()));

        Collection<VortexProperty<?>> properties = vertex.getProperties();
        E.checkArgument(properties.size() == 1,
                        "Expect only 1 property for olap vertex, but got %s",
                        properties.size());
        VortexProperty<?> property = properties.iterator().next();
        PropertyKey pk = property.propertyKey();
        entry.subId(pk.id());
        entry.column(VortexKeys.PROPERTY_VALUE,
                     this.writeProperty(pk, property.value()));
        entry.olap(true);
        return entry;
    }

    @Override
    protected Object writeProperty(PropertyKey propertyKey, Object value) {
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_PROPERTY);
        if (propertyKey == null) {
            /*
             * Since we can't know the type of the property value in some
             * scenarios so need to construct a fake property key to
             * serialize to reuse code.
             */
            propertyKey = new PropertyKey(null, IdGenerator.of(0L), "fake");
            propertyKey.dataType(DataType.fromClass(value.getClass()));
        }
        buffer.writeProperty(propertyKey, value);
        buffer.forReadWritten();
        return buffer.asByteBuffer();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T readProperty(PropertyKey pkey, Object value) {
        BytesBuffer buffer = BytesBuffer.wrap((ByteBuffer) value);
        return (T) buffer.readProperty(pkey);
    }

    @Override
    protected Object writeId(Id id) {
        return IdUtil.writeBinString(id);
    }

    @Override
    protected Id readId(Object id) {
        return IdUtil.readBinString(id);
    }

    @Override
    protected void writeUserdata(SchemaElement schema,
                                 TableBackendEntry entry) {
        assert entry instanceof CassandraBackendEntry;
        for (Map.Entry<String, Object> e : schema.userdata().entrySet()) {
            entry.column(VortexKeys.USER_DATA, e.getKey(),
                         JsonUtil.toJson(e.getValue()));
        }
    }

    @Override
    protected void readUserdata(SchemaElement schema,
                                TableBackendEntry entry) {
        assert entry instanceof CassandraBackendEntry;
        // Parse all user data of a schema element
        Map<String, String> userdata = entry.column(VortexKeys.USER_DATA);
        for (Map.Entry<String, String> e : userdata.entrySet()) {
            String key = e.getKey();
            Object value = JsonUtil.fromJson(e.getValue(), Object.class);
            schema.userdata(key, value);
        }
    }
}
