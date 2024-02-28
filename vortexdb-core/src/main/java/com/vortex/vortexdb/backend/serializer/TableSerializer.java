
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.EdgeId;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.id.IdUtil;
import com.vortex.vortexdb.backend.query.Condition;
import com.vortex.vortexdb.backend.query.ConditionQuery;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.schema.*;
import com.vortex.vortexdb.structure.*;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.*;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class TableSerializer extends AbstractSerializer {

    @Override
    public TableBackendEntry newBackendEntry(VortexType type, Id id) {
        return new TableBackendEntry(type, id);
    }

    protected TableBackendEntry newBackendEntry(VortexElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected TableBackendEntry newBackendEntry(SchemaElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected TableBackendEntry newBackendEntry(VortexIndex index) {
        return newBackendEntry(index.type(), index.id());
    }

    protected abstract TableBackendEntry newBackendEntry(TableBackendEntry.Row row);

    @Override
    protected abstract TableBackendEntry convertEntry(BackendEntry backendEntry);

    protected void formatProperty(VortexProperty<?> property,
                                  TableBackendEntry.Row row) {
        long pkid = property.propertyKey().id().asLong();
        row.column(VortexKeys.PROPERTIES, pkid, this.writeProperty(property));
    }

    protected void parseProperty(Id key, Object colValue, VortexElement owner) {
        // Get PropertyKey by PropertyKey id
        PropertyKey pkey = owner.graph().propertyKey(key);

        // Parse value
        Object value = this.readProperty(pkey, colValue);

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-single property: %s", value);
            }
            owner.addProperty(pkey, value);
        }
    }

    protected Object writeProperty(VortexProperty<?> property) {
        return this.writeProperty(property.propertyKey(), property.value());
    }

    protected Object writeProperty(PropertyKey propertyKey, Object value) {
        return JsonUtil.toJson(value);
    }

    @SuppressWarnings("unchecked")
    protected <T> T readProperty(PropertyKey pkey, Object value) {
        Class<T> clazz = (Class<T>) pkey.implementClazz();
        T result = JsonUtil.fromJson(value.toString(), clazz);
        if (pkey.cardinality() != Cardinality.SINGLE) {
            Collection<?> values = (Collection<?>) result;
            List<Object> newValues = new ArrayList<>(values.size());
            for (Object v : values) {
                newValues.add(JsonUtil.castNumber(v, pkey.dataType().clazz()));
            }
            result = (T) newValues;
        }
        return result;
    }

    protected TableBackendEntry.Row formatEdge(VortexEdge edge) {
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
            row.column(VortexKeys.EXPIRED_TIME, edge.expiredTime());
        }
        // Id: ownerVertex + direction + edge-label + sortValues + otherVertex
        row.column(VortexKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(VortexKeys.DIRECTION, id.directionCode());
        row.column(VortexKeys.LABEL, id.edgeLabelId().asLong());
        row.column(VortexKeys.SORT_VALUES, id.sortValues());
        row.column(VortexKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));

        this.formatProperties(edge, row);
        return row;
    }

    /**
     * Parse an edge from a entry row
     * @param row edge entry
     * @param vertex null or the source vertex
     * @param graph the Vortex context object
     * @return the source vertex
     */
    protected VortexEdge parseEdge(TableBackendEntry.Row row,
                                 VortexVertex vertex, Vortex graph) {
        Object ownerVertexId = row.column(VortexKeys.OWNER_VERTEX);
        Number dir = row.column(VortexKeys.DIRECTION);
        boolean direction = EdgeId.isOutDirectionFromCode(dir.byteValue());
        Number label = row.column(VortexKeys.LABEL);
        String sortValues = row.column(VortexKeys.SORT_VALUES);
        Object otherVertexId = row.column(VortexKeys.OTHER_VERTEX);
        Number expiredTime = row.column(VortexKeys.EXPIRED_TIME);

        if (vertex == null) {
            Id ownerId = this.readId(ownerVertexId);
            vertex = new VortexVertex(graph, ownerId, VertexLabel.NONE);
        }

        EdgeLabel edgeLabel = graph.edgeLabelOrNone(this.toId(label));
        Id otherId = this.readId(otherVertexId);

        // Construct edge
        VortexEdge edge = VortexEdge.constructEdge(vertex, direction, edgeLabel,
                                               sortValues, otherId);

        // Parse edge properties
        this.parseProperties(edge, row);

        // The expired time is null when the edge is non-ttl
        long expired = edge.hasTtl() ? expiredTime.longValue() : 0L;
        edge.expiredTime(expired);

        return edge;
    }

    @Override
    public BackendEntry writeVertex(VortexVertex vertex) {
        if (vertex.olap()) {
            return this.writeOlapVertex(vertex);
        }
        TableBackendEntry entry = newBackendEntry(vertex);
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
            entry.column(VortexKeys.EXPIRED_TIME, vertex.expiredTime());
        }
        entry.column(VortexKeys.ID, this.writeId(vertex.id()));
        entry.column(VortexKeys.LABEL, vertex.schemaLabel().id().asLong());
        // Add all properties of a Vertex
        this.formatProperties(vertex, entry.row());
        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(VortexVertexProperty<?> prop) {
        VortexVertex vertex = prop.element();
        TableBackendEntry entry = newBackendEntry(vertex);
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
            entry.column(VortexKeys.EXPIRED_TIME, vertex.expiredTime());
        }
        entry.subId(IdGenerator.of(prop.key()));
        entry.column(VortexKeys.ID, this.writeId(vertex.id()));
        entry.column(VortexKeys.LABEL, vertex.schemaLabel().id().asLong());

        this.formatProperty(prop, entry.row());
        return entry;
    }

    @Override
    public VortexVertex readVertex(Vortex graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);
        assert entry.type().isVertex();

        Id id = this.readId(entry.column(VortexKeys.ID));
        Number label = entry.column(VortexKeys.LABEL);
        Number expiredTime = entry.column(VortexKeys.EXPIRED_TIME);

        VertexLabel vertexLabel = VertexLabel.NONE;
        if (label != null) {
            vertexLabel = graph.vertexLabelOrNone(this.toId(label));
        }
        VortexVertex vertex = new VortexVertex(graph, id, vertexLabel);

        // Parse all properties of a Vertex
        this.parseProperties(vertex, entry.row());
        // Parse all edges of a Vertex
        for (TableBackendEntry.Row edge : entry.subRows()) {
            this.parseEdge(edge, vertex, graph);
        }
        // The expired time is null when this is fake vertex of edge or non-ttl
        if (expiredTime != null) {
            vertex.expiredTime(expiredTime.longValue());
        }
        return vertex;
    }

    @Override
    public BackendEntry writeEdge(VortexEdge edge) {
        return newBackendEntry(this.formatEdge(edge));
    }

    @Override
    public BackendEntry writeEdgeProperty(VortexEdgeProperty<?> prop) {
        VortexEdge edge = prop.element();
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
            row.column(VortexKeys.EXPIRED_TIME, edge.expiredTime());
        }
        // Id: ownerVertex + direction + edge-label + sortValues + otherVertex
        row.column(VortexKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(VortexKeys.DIRECTION, id.directionCode());
        row.column(VortexKeys.LABEL, id.edgeLabelId().asLong());
        row.column(VortexKeys.SORT_VALUES, id.sortValues());
        row.column(VortexKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));

        // Format edge property
        this.formatProperty(prop, row);

        TableBackendEntry entry = newBackendEntry(row);
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }

    @Override
    public VortexEdge readEdge(Vortex graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);
        return this.parseEdge(entry.row(), null, graph);
    }

    @Override
    public BackendEntry writeIndex(VortexIndex index) {
        TableBackendEntry entry = newBackendEntry(index);
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/range index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(VortexKeys.INDEX_LABEL_ID, index.indexLabel().longId());
        } else {
            entry.column(VortexKeys.FIELD_VALUES, index.fieldValues());
            entry.column(VortexKeys.INDEX_LABEL_ID, index.indexLabel().longId());
            entry.column(VortexKeys.ELEMENT_IDS, this.writeId(index.elementId()));
            entry.subId(index.elementId());
            if (index.hasTtl()) {
                entry.ttl(index.ttl());
                entry.column(VortexKeys.EXPIRED_TIME, index.expiredTime());
            }
        }
        return entry;
    }

    @Override
    public VortexIndex readIndex(Vortex graph, ConditionQuery query,
                               BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Object indexValues = entry.column(VortexKeys.FIELD_VALUES);
        Number indexLabelId = entry.column(VortexKeys.INDEX_LABEL_ID);
        Set<Object> elemIds = this.parseIndexElemIds(entry);
        Number expiredTime = entry.column(VortexKeys.EXPIRED_TIME);

        IndexLabel indexLabel = graph.indexLabel(this.toId(indexLabelId));
        VortexIndex index = new VortexIndex(graph, indexLabel);
        index.fieldValues(indexValues);
        long expired = index.hasTtl() ? expiredTime.longValue() : 0L;
        for (Object elemId : elemIds) {
            index.elementIds(this.readId(elemId), expired);
        }
        return index;
    }

    @Override
    public BackendEntry writeId(VortexType type, Id id) {
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(VortexType type, Id id) {
        if (type.isEdge()) {
            if (!(id instanceof EdgeId)) {
                id = EdgeId.parse(id.asString());
            }
        } else if (type.isGraph()) {
            id = IdGenerator.of(this.writeId(id));
        }
        return id;
    }

    @Override
    protected Query writeQueryEdgeCondition(Query query) {
        query = this.writeQueryCondition(query);
        return query.idsSize() == 0 ? query : null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        ConditionQuery result = (ConditionQuery) query;
        // No user-prop when serialize
        assert result.allSysprop();
        for (Condition.Relation r : result.relations()) {
            if (!r.value().equals(r.serialValue())) {
                // Has been serialized before (maybe share a query multi times)
                continue;
            }
            VortexKeys key = (VortexKeys) r.key();
            if (r.relation() == Condition.RelationType.IN) {
                E.checkArgument(r.value() instanceof List,
                                "Expect list value for IN condition: %s", r);
                List<?> values = (List<?>) r.value();
                List<Object> serializedValues = new ArrayList<>(values.size());
                for (Object v : values) {
                    serializedValues.add(this.serializeValue(key, v));
                }
                r.serialValue(serializedValues);
            } else if (r.relation() == Condition.RelationType.CONTAINS_VALUE &&
                       query.resultType().isGraph()) {
                r.serialValue(this.writeProperty(null, r.value()));
            } else {
                r.serialValue(this.serializeValue(key, r.value()));
            }
        }

        return result;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        TableBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(VortexKeys.ID, vertexLabel.id().asLong());
        entry.column(VortexKeys.NAME, vertexLabel.name());
        entry.column(VortexKeys.ID_STRATEGY, vertexLabel.idStrategy().code());
        entry.column(VortexKeys.PROPERTIES,
                     this.toLongSet(vertexLabel.properties()));
        entry.column(VortexKeys.PRIMARY_KEYS,
                     this.toLongList(vertexLabel.primaryKeys()));
        entry.column(VortexKeys.NULLABLE_KEYS,
                     this.toLongSet(vertexLabel.nullableKeys()));
        entry.column(VortexKeys.INDEX_LABELS,
                     this.toLongSet(vertexLabel.indexLabels()));
        this.writeEnableLabelIndex(vertexLabel, entry);
        this.writeUserdata(vertexLabel, entry);
        entry.column(VortexKeys.STATUS, vertexLabel.status().code());
        entry.column(VortexKeys.TTL, vertexLabel.ttl());
        entry.column(VortexKeys.TTL_START_TIME,
                     vertexLabel.ttlStartTime().asLong());
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        TableBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(VortexKeys.ID, edgeLabel.id().asLong());
        entry.column(VortexKeys.NAME, edgeLabel.name());
        entry.column(VortexKeys.FREQUENCY, edgeLabel.frequency().code());
        entry.column(VortexKeys.SOURCE_LABEL, edgeLabel.sourceLabel().asLong());
        entry.column(VortexKeys.TARGET_LABEL, edgeLabel.targetLabel().asLong());
        entry.column(VortexKeys.PROPERTIES,
                     this.toLongSet(edgeLabel.properties()));
        entry.column(VortexKeys.SORT_KEYS,
                     this.toLongList(edgeLabel.sortKeys()));
        entry.column(VortexKeys.NULLABLE_KEYS,
                     this.toLongSet(edgeLabel.nullableKeys()));
        entry.column(VortexKeys.INDEX_LABELS,
                     this.toLongSet(edgeLabel.indexLabels()));
        this.writeEnableLabelIndex(edgeLabel, entry);
        this.writeUserdata(edgeLabel, entry);
        entry.column(VortexKeys.STATUS, edgeLabel.status().code());
        entry.column(VortexKeys.TTL, edgeLabel.ttl());
        entry.column(VortexKeys.TTL_START_TIME,
                     edgeLabel.ttlStartTime().asLong());
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        TableBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(VortexKeys.ID, propertyKey.id().asLong());
        entry.column(VortexKeys.NAME, propertyKey.name());
        entry.column(VortexKeys.DATA_TYPE, propertyKey.dataType().code());
        entry.column(VortexKeys.CARDINALITY, propertyKey.cardinality().code());
        entry.column(VortexKeys.AGGREGATE_TYPE,
                     propertyKey.aggregateType().code());
        entry.column(VortexKeys.WRITE_TYPE,
                     propertyKey.writeType().code());
        entry.column(VortexKeys.PROPERTIES,
                     this.toLongSet(propertyKey.properties()));
        this.writeUserdata(propertyKey, entry);
        entry.column(VortexKeys.STATUS, propertyKey.status().code());
        return entry;
    }

    @Override
    public VertexLabel readVertexLabel(Vortex graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, VortexKeys.ID);
        String name = schemaColumn(entry, VortexKeys.NAME);
        IdStrategy idStrategy = schemaEnum(entry, VortexKeys.ID_STRATEGY,
                                           IdStrategy.class);
        Object properties = schemaColumn(entry, VortexKeys.PROPERTIES);
        Object primaryKeys = schemaColumn(entry, VortexKeys.PRIMARY_KEYS);
        Object nullableKeys = schemaColumn(entry, VortexKeys.NULLABLE_KEYS);
        Object indexLabels = schemaColumn(entry, VortexKeys.INDEX_LABELS);
        SchemaStatus status = schemaEnum(entry, VortexKeys.STATUS,
                                         SchemaStatus.class);
        Number ttl = schemaColumn(entry, VortexKeys.TTL);
        Number ttlStartTime = schemaColumn(entry, VortexKeys.TTL_START_TIME);

        VertexLabel vertexLabel = new VertexLabel(graph, this.toId(id), name);
        vertexLabel.idStrategy(idStrategy);
        vertexLabel.properties(this.toIdArray(properties));
        vertexLabel.primaryKeys(this.toIdArray(primaryKeys));
        vertexLabel.nullableKeys(this.toIdArray(nullableKeys));
        vertexLabel.indexLabels(this.toIdArray(indexLabels));
        vertexLabel.status(status);
        vertexLabel.ttl(ttl.longValue());
        vertexLabel.ttlStartTime(this.toId(ttlStartTime));
        this.readEnableLabelIndex(vertexLabel, entry);
        this.readUserdata(vertexLabel, entry);
        return vertexLabel;
    }

    @Override
    public EdgeLabel readEdgeLabel(Vortex graph, BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, VortexKeys.ID);
        String name = schemaColumn(entry, VortexKeys.NAME);
        // Frequency frequency = schemaEnum(entry, VortexKeys.FREQUENCY, Frequency.class);
        Number sourceLabel = schemaColumn(entry, VortexKeys.SOURCE_LABEL);
        Number targetLabel = schemaColumn(entry, VortexKeys.TARGET_LABEL);
        Object sortKeys = schemaColumn(entry, VortexKeys.SORT_KEYS);
        Object nullableKeys = schemaColumn(entry, VortexKeys.NULLABLE_KEYS);
        Object properties = schemaColumn(entry, VortexKeys.PROPERTIES);
        Object indexLabels = schemaColumn(entry, VortexKeys.INDEX_LABELS);
        SchemaStatus status = schemaEnum(entry, VortexKeys.STATUS,
                                         SchemaStatus.class);
        Number ttl = schemaColumn(entry, VortexKeys.TTL);
        Number ttlStartTime = schemaColumn(entry, VortexKeys.TTL_START_TIME);

        EdgeLabel edgeLabel = new EdgeLabel(graph, this.toId(id), name);
        // edgeLabel.frequency(frequency);
        edgeLabel.sourceLabel(this.toId(sourceLabel));
        edgeLabel.targetLabel(this.toId(targetLabel));
        edgeLabel.properties(this.toIdArray(properties));
        edgeLabel.sortKeys(this.toIdArray(sortKeys));
        edgeLabel.nullableKeys(this.toIdArray(nullableKeys));
        edgeLabel.indexLabels(this.toIdArray(indexLabels));
        edgeLabel.status(status);
        edgeLabel.ttl(ttl.longValue());
        edgeLabel.ttlStartTime(this.toId(ttlStartTime));
        this.readEnableLabelIndex(edgeLabel, entry);
        this.readUserdata(edgeLabel, entry);
        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(Vortex graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, VortexKeys.ID);
        String name = schemaColumn(entry, VortexKeys.NAME);
        //DataType dataType = schemaEnum(entry, VortexKeys.DATA_TYPE, DataType.class);
        Cardinality cardinality = schemaEnum(entry, VortexKeys.CARDINALITY, Cardinality.class);
        AggregateType aggregateType = schemaEnum(entry, VortexKeys.AGGREGATE_TYPE, AggregateType.class);
        WriteType writeType = schemaEnumOrDefault(
                              entry, VortexKeys.WRITE_TYPE,
                              WriteType.class, WriteType.OLTP);
        Object properties = schemaColumn(entry, VortexKeys.PROPERTIES);
        //SchemaStatus status = schemaEnum(entry, VortexKeys.STATUS,SchemaStatus.class);

        PropertyKey propertyKey = new PropertyKey(graph, this.toId(id), name);
        //propertyKey.dataType(dataType);
        propertyKey.cardinality(cardinality);
        propertyKey.aggregateType(aggregateType);
        propertyKey.writeType(writeType);
        propertyKey.properties(this.toIdArray(properties));
        //propertyKey.status(status);
        this.readUserdata(propertyKey, entry);
        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        TableBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(VortexKeys.ID, indexLabel.id().asLong());
        entry.column(VortexKeys.NAME, indexLabel.name());
        entry.column(VortexKeys.BASE_TYPE, indexLabel.baseType().code());
        entry.column(VortexKeys.BASE_VALUE, indexLabel.baseValue().asLong());
        entry.column(VortexKeys.INDEX_TYPE, indexLabel.indexType().code());
        entry.column(VortexKeys.FIELDS,
                     this.toLongList(indexLabel.indexFields()));
        this.writeUserdata(indexLabel, entry);
        entry.column(VortexKeys.STATUS, indexLabel.status().code());
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(Vortex graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = schemaColumn(entry, VortexKeys.ID);
        String name = schemaColumn(entry, VortexKeys.NAME);
        VortexType baseType = schemaEnum(entry, VortexKeys.BASE_TYPE,
                                       VortexType.class);
        Number baseValueId = schemaColumn(entry, VortexKeys.BASE_VALUE);
        //IndexType indexType = schemaEnum(entry, VortexKeys.INDEX_TYPE,
        //                                 IndexType.class);
        Object indexFields = schemaColumn(entry, VortexKeys.FIELDS);
        SchemaStatus status = schemaEnum(entry, VortexKeys.STATUS,
                                         SchemaStatus.class);

        IndexLabel indexLabel = new IndexLabel(graph, this.toId(id), name);
        indexLabel.baseType(baseType);
        indexLabel.baseValue(this.toId(baseValueId));
        //indexLabel.indexType(indexType);
        indexLabel.indexFields(this.toIdArray(indexFields));
        indexLabel.status(status);
        this.readUserdata(indexLabel, entry);
        return indexLabel;
    }

    protected abstract Id toId(Number number);

    protected abstract Id[] toIdArray(Object object);

    protected abstract Object toLongSet(Collection<Id> ids);

    protected abstract Object toLongList(Collection<Id> ids);

    protected abstract Set<Object> parseIndexElemIds(TableBackendEntry entry);

    protected abstract void formatProperties(VortexElement element,
                                             TableBackendEntry.Row row);

    protected abstract void parseProperties(VortexElement element,
                                            TableBackendEntry.Row row);

    protected Object writeId(Id id) {
        return IdUtil.writeStoredString(id);
    }

    protected Id readId(Object id) {
        return IdUtil.readStoredString(id.toString());
    }

    protected Object serializeValue(VortexKeys key, Object value) {
        if (value instanceof Directions) {
            value = ((Directions) value).type().code();
        } else if (value instanceof Id) {
            if (key == VortexKeys.OWNER_VERTEX || key == VortexKeys.OTHER_VERTEX) {
                // Serialize vertex id
                value = this.writeId((Id) value);
            } else {
                // Serialize other id value, like label id
                value = ((Id) value).asObject();
            }
        }

        return value;
    }

    protected void writeEnableLabelIndex(SchemaLabel schema,
                                         TableBackendEntry entry) {
        entry.column(VortexKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
    }

    protected void readEnableLabelIndex(SchemaLabel schema,
                                        TableBackendEntry entry) {
        Boolean enableLabelIndex = schemaColumn(entry,
                                                VortexKeys.ENABLE_LABEL_INDEX);
        schema.enableLabelIndex(enableLabelIndex);
    }

    protected abstract void writeUserdata(SchemaElement schema,
                                          TableBackendEntry entry);

    protected abstract void readUserdata(SchemaElement schema,
                                         TableBackendEntry entry);

    private static <T> T schemaColumn(TableBackendEntry entry, VortexKeys key) {
        assert entry.type().isSchema();

        T value = entry.column(key);
        E.checkState(value != null,
                     "Not found key '%s' from entry %s", key, entry);
        return value;
    }

    private static <T extends SerialEnum> T schemaEnum(TableBackendEntry entry,
                                                       VortexKeys key,
                                                       Class<T> clazz) {
        Number value = schemaColumn(entry, key);
        return SerialEnum.fromCode(clazz, value.byteValue());
    }

    private static <T extends SerialEnum> T schemaEnumOrDefault(
                                            TableBackendEntry entry,
                                            VortexKeys key, Class<T> clazz,
                                            T defaultValue) {
        assert entry.type().isSchema();

        Number value = entry.column(key);
        if (value == null) {
            return defaultValue;
        }
        return SerialEnum.fromCode(clazz, value.byteValue());
    }
}
