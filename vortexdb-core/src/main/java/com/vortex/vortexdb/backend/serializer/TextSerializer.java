
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.*;
import com.vortex.vortexdb.backend.query.*;
import com.vortex.vortexdb.backend.query.Condition.RangeConditions;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.schema.*;
import com.vortex.vortexdb.structure.*;
import com.vortex.vortexdb.structure.VortexIndex.IdWithExpiredTime;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.*;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.JsonUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.NotImplementedException;

import java.util.*;

public class TextSerializer extends AbstractSerializer {

    private static final String VALUE_SPLITOR = TextBackendEntry.VALUE_SPLITOR;
    private static final String EDGE_NAME_ENDING =
                                ConditionQuery.INDEX_SYM_ENDING;

    private static final String EDGE_OUT_TYPE = writeType(VortexType.EDGE_OUT);

    @Override
    public TextBackendEntry newBackendEntry(VortexType type, Id id) {
        return new TextBackendEntry(type, id);
    }

    private TextBackendEntry newBackendEntry(VortexElement elem) {
        Id id = IdGenerator.of(writeEntryId(elem.id()));
        return new TextBackendEntry(elem.type(), id);
    }

    private TextBackendEntry newBackendEntry(SchemaElement elem) {
        Id id = IdGenerator.of(writeId(elem.id()));
        return new TextBackendEntry(elem.type(), id);
    }

    @Override
    protected TextBackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof TextBackendEntry)) {
            throw new VortexException("The entry '%s' is not TextBackendEntry",
                                    backendEntry);
        }
        return (TextBackendEntry) backendEntry;
    }

    private String formatSyspropName(String name) {
        return SplicingIdGenerator.concat(writeType(VortexType.SYS_PROPERTY),
                                          name);
    }

    private String formatSyspropName(VortexKeys col) {
        return this.formatSyspropName(col.string());
    }

    private String formatPropertyName(String key) {
        return SplicingIdGenerator.concat(writeType(VortexType.PROPERTY), key);
    }

    private String formatPropertyName(VortexProperty<?> prop) {
        return this.formatPropertyName(writeId(prop.propertyKey().id()));
    }

    private String formatPropertyValue(VortexProperty<?> prop) {
        // May be a single value or a list of values
        return JsonUtil.toJson(prop.value());
    }

    private String formatPropertyName() {
        return VortexType.PROPERTY.string();
    }

    private String formatPropertyValues(VortexVertex vertex) {
        int size = vertex.sizeOfProperties();
        StringBuilder sb = new StringBuilder(64 * size);
        // Vertex properties
        int i = 0;
        for (VortexProperty<?> property : vertex.getProperties()) {
            sb.append(this.formatPropertyName(property));
            sb.append(VALUE_SPLITOR);
            sb.append(this.formatPropertyValue(property));
            if (++i < size) {
                sb.append(VALUE_SPLITOR);
            }
        }
        return sb.toString();
    }

    private void parseProperty(String colName, String colValue,
                               VortexElement owner) {
        String[] colParts = SplicingIdGenerator.split(colName);
        assert colParts.length == 2 : colName;

        // Get PropertyKey by PropertyKey id
        PropertyKey pkey = owner.graph().propertyKey(readId(colParts[1]));

        // Parse value
        Object value = JsonUtil.fromJson(colValue, pkey.implementClazz());

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-sigle property: %s", colValue);
            }
            for (Object v : (Collection<?>) value) {
                v = JsonUtil.castNumber(v, pkey.dataType().clazz());
                owner.addProperty(pkey, v);
            }
        }
    }

    private void parseProperties(String colValue, VortexVertex vertex) {
        if (colValue == null || colValue.isEmpty()) {
            return;
        }
        String[] valParts = colValue.split(VALUE_SPLITOR);
        E.checkState(valParts.length % 2 == 0,
                     "The property key values length must be even number, " +
                     "but got %s, length is '%s'",
                     Arrays.toString(valParts), valParts.length);
        // Edge properties
        for (int i = 0; i < valParts.length; i += 2) {
            assert i + 1 < valParts.length;
            this.parseProperty(valParts[i], valParts[i + 1], vertex);
        }
    }

    private String formatEdgeName(VortexEdge edge) {
        // Edge name: type + edge-label-name + sortKeys + targetVertex
        return writeEdgeId(edge.idWithDirection(), false);
    }

    private String formatEdgeValue(VortexEdge edge) {
        StringBuilder sb = new StringBuilder(256 * edge.sizeOfProperties());
        // Edge id
        sb.append(edge.id().asString());
        // Write edge expired time
        sb.append(VALUE_SPLITOR);
        sb.append(this.formatSyspropName(VortexKeys.EXPIRED_TIME));
        sb.append(VALUE_SPLITOR);
        sb.append(edge.expiredTime());
        // Edge properties
        for (VortexProperty<?> property : edge.getProperties()) {
            sb.append(VALUE_SPLITOR);
            sb.append(this.formatPropertyName(property));
            sb.append(VALUE_SPLITOR);
            sb.append(this.formatPropertyValue(property));
        }
        return sb.toString();
    }

    /**
     * Parse an edge from a column item
     */
    private void parseEdge(String colName, String colValue,
                           VortexVertex vertex) {
        String[] colParts = EdgeId.split(colName);

        Vortex graph = vertex.graph();
        boolean direction = colParts[0].equals(EDGE_OUT_TYPE);
        String sortValues = readEdgeName(colParts[2]);
        EdgeLabel edgeLabel = graph.edgeLabelOrNone(readId(colParts[1]));
        Id otherVertexId = readEntryId(colParts[3]);
        // Construct edge
        VortexEdge edge = VortexEdge.constructEdge(vertex, direction, edgeLabel,
                                               sortValues, otherVertexId);

        String[] valParts = colValue.split(VALUE_SPLITOR);
        // Parse edge expired time
        String name = this.formatSyspropName(VortexKeys.EXPIRED_TIME);
        E.checkState(valParts[1].equals(name),
                     "Invalid system property name '%s'", valParts[1]);
        edge.expiredTime(JsonUtil.fromJson(valParts[2], Long.class));

        // Edge properties
        for (int i = 3; i < valParts.length; i += 2) {
            this.parseProperty(valParts[i], valParts[i + 1], edge);
        }
    }

    private void parseColumn(String colName, String colValue,
                             VortexVertex vertex) {
        // Column name
        String type = SplicingIdGenerator.split(colName)[0];
        // Parse property
        if (type.equals(writeType(VortexType.PROPERTY))) {
            this.parseProperties(colValue, vertex);
        }
        // Parse edge
        else if (type.equals(writeType(VortexType.EDGE_OUT)) ||
                 type.equals(writeType(VortexType.EDGE_IN))) {
            this.parseEdge(colName, colValue, vertex);
        }
        // Parse system property
        else if (type.equals(writeType(VortexType.SYS_PROPERTY))) {
            // pass
        }
        // Invalid entry
        else {
            E.checkState(false, "Invalid entry with unknown type(%s): %s",
                         type, colName);
        }
    }

    @Override
    public BackendEntry writeVertex(VortexVertex vertex) {
        TextBackendEntry entry = newBackendEntry(vertex);

        // Write label (NOTE: maybe just with edges if label is null)
        if (vertex.schemaLabel() != null) {
            entry.column(this.formatSyspropName(VortexKeys.LABEL),
                         writeId(vertex.schemaLabel().id()));
        }

        // Write expired time
        entry.column(this.formatSyspropName(VortexKeys.EXPIRED_TIME),
                     writeLong(vertex.expiredTime()));
        // Add all properties of a Vertex
        entry.column(this.formatPropertyName(),
                     this.formatPropertyValues(vertex));
        return entry;
    }

    @Override
    public BackendEntry writeOlapVertex(VortexVertex vertex) {
        throw new NotImplementedException("Unsupported writeOlapVertex()");
    }

    @Override
    public BackendEntry writeVertexProperty(VortexVertexProperty<?> prop) {
        throw new NotImplementedException("Unsupported writeVertexProperty()");
    }

    @Override
    public VortexVertex readVertex(Vortex graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        // Parse label
        String labelId = entry.column(this.formatSyspropName(VortexKeys.LABEL));
        VertexLabel vertexLabel = VertexLabel.NONE;
        if (labelId != null) {
            vertexLabel = graph.vertexLabelOrNone(readId(labelId));
        }

        Id id = IdUtil.readString(entry.id().asString());
        VortexVertex vertex = new VortexVertex(graph, id, vertexLabel);

        String expiredTime = entry.column(this.formatSyspropName(
                             VortexKeys.EXPIRED_TIME));
        // Expired time is null when backend entry is fake vertex with edges
        if (expiredTime != null) {
            vertex.expiredTime(readLong(expiredTime));
        }

        // Parse all properties or edges of a Vertex
        for (String name : entry.columnNames()) {
            this.parseColumn(name, entry.column(name), vertex);
        }

        return vertex;
    }

    @Override
    public BackendEntry writeEdge(VortexEdge edge) {
        Id id = IdGenerator.of(edge.idWithDirection().asString());
        TextBackendEntry entry = newBackendEntry(edge.type(), id);
        entry.column(this.formatEdgeName(edge), this.formatEdgeValue(edge));
        return entry;
    }

    @Override
    public BackendEntry writeEdgeProperty(VortexEdgeProperty<?> prop) {
        VortexEdge edge = prop.element();
        Id id = IdGenerator.of(edge.idWithDirection().asString());
        TextBackendEntry entry = newBackendEntry(edge.type(), id);
        entry.subId(IdGenerator.of(prop.key()));
        entry.column(this.formatEdgeName(edge), this.formatEdgeValue(edge));
        return entry;
    }

    @Override
    public VortexEdge readEdge(Vortex graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        // TODO: implement
        throw new NotImplementedException("Unsupported readEdge()");
    }

    @Override
    public BackendEntry writeIndex(VortexIndex index) {
        TextBackendEntry entry = newBackendEntry(index.type(), index.id());
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            /*
             * When field-values is null and elementIds size is 0, it is
             * meaningful for deletion of index data in secondary/range index.
             */
            entry.column(VortexKeys.INDEX_LABEL_ID,
                         writeId(index.indexLabelId()));
        } else {
            // TODO: field-values may be a number (range index)
            entry.column(formatSyspropName(VortexKeys.FIELD_VALUES),
                         JsonUtil.toJson(index.fieldValues()));
            entry.column(formatSyspropName(VortexKeys.INDEX_LABEL_ID),
                         writeId(index.indexLabelId()));
            entry.column(formatSyspropName(VortexKeys.ELEMENT_IDS),
                         writeElementId(index.elementId(), index.expiredTime()));
            entry.subId(index.elementId());
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

        TextBackendEntry entry = this.convertEntry(backendEntry);
        String indexValues = entry.column(
                             formatSyspropName(VortexKeys.FIELD_VALUES));
        String indexLabelId = entry.column(
                              formatSyspropName(VortexKeys.INDEX_LABEL_ID));
        String elemIds = entry.column(
                         formatSyspropName(VortexKeys.ELEMENT_IDS));

        IndexLabel indexLabel = IndexLabel.label(graph, readId(indexLabelId));
        VortexIndex index = new VortexIndex(graph, indexLabel);
        index.fieldValues(JsonUtil.fromJson(indexValues, Object.class));
        for (IdWithExpiredTime elemId : readElementIds(elemIds)) {
            long expiredTime = elemId.expiredTime();
            Id id;
            if (indexLabel.queryType().isEdge()) {
                id = EdgeId.parse(elemId.id().asString());
            } else {
                id = elemId.id();
            }
            index.elementIds(id, expiredTime);
        }
        // Memory backend might return empty BackendEntry
        return index;
    }

    @Override
    public TextBackendEntry writeId(VortexType type, Id id) {
        id = this.writeQueryId(type, id);
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(VortexType type, Id id) {
        if (type.isEdge()) {
            id = IdGenerator.of(writeEdgeId(id, true));
        } else if (type.isGraph()) {
            id = IdGenerator.of(writeEntryId(id));
        } else {
            assert type.isSchema();
            id = IdGenerator.of(writeId(id));
        }
        return id;
    }

    @Override
    protected Query writeQueryEdgeCondition(Query query) {
        ConditionQuery cq = (ConditionQuery) query;
        if (cq.hasRangeCondition()) {
            return this.writeQueryEdgeRangeCondition(cq);
        } else {
            return this.writeQueryEdgePrefixCondition(cq);
        }
    }

    private Query writeQueryEdgeRangeCondition(ConditionQuery cq) {
        List<Condition> sortValues = cq.syspropConditions(VortexKeys.SORT_VALUES);
        E.checkArgument(sortValues.size() >= 1 && sortValues.size() <= 2,
                        "Edge range query must be with sort-values range");
        // Would ignore target vertex
        Object vertex = cq.condition(VortexKeys.OWNER_VERTEX);
        Object direction = cq.condition(VortexKeys.DIRECTION);
        if (direction == null) {
            direction = Directions.OUT;
        }
        Object label = cq.condition(VortexKeys.LABEL);

        List<String> start = new ArrayList<>(cq.conditionsSize());
        start.add(writeEntryId((Id) vertex));
        start.add(writeType(((Directions) direction).type()));
        start.add(writeId((Id) label));

        List<String> end = new ArrayList<>(start);

        RangeConditions range = new RangeConditions(sortValues);
        if (range.keyMin() != null) {
            start.add((String) range.keyMin());
        }
        if (range.keyMax() != null) {
            end.add((String) range.keyMax());
        }

        // Sort-value will be empty if there is no start sort-value
        String startId = EdgeId.concat(start.toArray(new String[0]));
        // Set endId as prefix if there is no end sort-value
        String endId = EdgeId.concat(end.toArray(new String[0]));
        if (range.keyMax() == null) {
            return new IdPrefixQuery(cq, IdGenerator.of(startId),
                                     range.keyMinEq(), IdGenerator.of(endId));
        }
        return new IdRangeQuery(cq, IdGenerator.of(startId), range.keyMinEq(),
                                IdGenerator.of(endId), range.keyMaxEq());
    }

    private Query writeQueryEdgePrefixCondition(ConditionQuery cq) {
        // Convert query-by-condition to query-by-id
        List<String> condParts = new ArrayList<>(cq.conditionsSize());

        for (VortexKeys key : EdgeId.KEYS) {
            Object value = cq.condition(key);
            if (value == null) {
                break;
            }
            // Serialize condition value
            if (key == VortexKeys.OWNER_VERTEX || key == VortexKeys.OTHER_VERTEX) {
                condParts.add(writeEntryId((Id) value));
            } else if (key == VortexKeys.DIRECTION) {
                condParts.add(writeType(((Directions) value).type()));
            } else if (key == VortexKeys.LABEL) {
                condParts.add(writeId((Id) value));
            } else {
                condParts.add(value.toString());
            }
        }

        if (condParts.size() > 0) {
            // Conditions to id
            String id = EdgeId.concat(condParts.toArray(new String[0]));
            return new IdPrefixQuery(cq, IdGenerator.of(id));
        }

        return null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        ConditionQuery result = (ConditionQuery) query;
        // No user-prop when serialize
        assert result.allSysprop();
        for (Condition.Relation r : result.relations()) {
            // Serialize key
            if (query.resultType().isSchema()) {
                r.serialKey(((VortexKeys) r.key()).string());
            } else {
                r.serialKey(formatSyspropName((VortexKeys) r.key()));
            }

            if (r.value() instanceof Id) {
                // Serialize id value
                r.serialValue(writeId((Id) r.value()));
            } else {
                // Serialize other type value
                r.serialValue(JsonUtil.toJson(r.value()));
            }

            if (r.relation() == Condition.RelationType.CONTAINS_KEY) {
                // Serialize has-key
                String key = (String) r.serialValue();
                r.serialValue(formatPropertyName(key));
            }
        }
        return result;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        TextBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(VortexKeys.NAME, JsonUtil.toJson(vertexLabel.name()));
        entry.column(VortexKeys.ID_STRATEGY,
                     JsonUtil.toJson(vertexLabel.idStrategy()));
        entry.column(VortexKeys.PROPERTIES,
                     writeIds(vertexLabel.properties()));
        entry.column(VortexKeys.PRIMARY_KEYS,
                     writeIds(vertexLabel.primaryKeys()));
        entry.column(VortexKeys.NULLABLE_KEYS,
                     writeIds(vertexLabel.nullableKeys()));
        entry.column(VortexKeys.INDEX_LABELS,
                     writeIds(vertexLabel.indexLabels()));
        entry.column(VortexKeys.ENABLE_LABEL_INDEX,
                     JsonUtil.toJson(vertexLabel.enableLabelIndex()));
        writeUserdata(vertexLabel, entry);
        entry.column(VortexKeys.STATUS,
                     JsonUtil.toJson(vertexLabel.status()));
        return entry;
    }

    @Override
    public VertexLabel readVertexLabel(Vortex graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        Id id = readId(entry.id());
        String name = JsonUtil.fromJson(entry.column(VortexKeys.NAME),
                                        String.class);
        String idStrategy = entry.column(VortexKeys.ID_STRATEGY);
        String properties = entry.column(VortexKeys.PROPERTIES);
        String primaryKeys = entry.column(VortexKeys.PRIMARY_KEYS);
        String nullableKeys = entry.column(VortexKeys.NULLABLE_KEYS);
        String indexLabels = entry.column(VortexKeys.INDEX_LABELS);
        String enableLabelIndex = entry.column(VortexKeys.ENABLE_LABEL_INDEX);
        String status = entry.column(VortexKeys.STATUS);

        VertexLabel vertexLabel = new VertexLabel(graph, id, name);
        vertexLabel.idStrategy(JsonUtil.fromJson(idStrategy,
                                                 IdStrategy.class));
        vertexLabel.properties(readIds(properties));
        vertexLabel.primaryKeys(readIds(primaryKeys));
        vertexLabel.nullableKeys(readIds(nullableKeys));
        vertexLabel.indexLabels(readIds(indexLabels));
        vertexLabel.enableLabelIndex(JsonUtil.fromJson(enableLabelIndex,
                                                       Boolean.class));
        readUserdata(vertexLabel, entry);
        vertexLabel.status(JsonUtil.fromJson(status, SchemaStatus.class));
        return vertexLabel;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        TextBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(VortexKeys.NAME, JsonUtil.toJson(edgeLabel.name()));
        entry.column(VortexKeys.SOURCE_LABEL, writeId(edgeLabel.sourceLabel()));
        entry.column(VortexKeys.TARGET_LABEL, writeId(edgeLabel.targetLabel()));
        entry.column(VortexKeys.FREQUENCY,
                     JsonUtil.toJson(edgeLabel.frequency()));
        entry.column(VortexKeys.PROPERTIES, writeIds(edgeLabel.properties()));
        entry.column(VortexKeys.SORT_KEYS, writeIds(edgeLabel.sortKeys()));
        entry.column(VortexKeys.NULLABLE_KEYS,
                     writeIds(edgeLabel.nullableKeys()));
        entry.column(VortexKeys.INDEX_LABELS, writeIds(edgeLabel.indexLabels()));
        entry.column(VortexKeys.ENABLE_LABEL_INDEX,
                     JsonUtil.toJson(edgeLabel.enableLabelIndex()));
        writeUserdata(edgeLabel, entry);
        entry.column(VortexKeys.STATUS,
                     JsonUtil.toJson(edgeLabel.status()));
        entry.column(VortexKeys.TTL, JsonUtil.toJson(edgeLabel.ttl()));
        entry.column(VortexKeys.TTL_START_TIME,
                     writeId(edgeLabel.ttlStartTime()));
        return entry;
    }

    @Override
    public EdgeLabel readEdgeLabel(Vortex graph,
                                   BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        Id id = readId(entry.id());
        String name = JsonUtil.fromJson(entry.column(VortexKeys.NAME),
                                        String.class);
        String sourceLabel = entry.column(VortexKeys.SOURCE_LABEL);
        String targetLabel = entry.column(VortexKeys.TARGET_LABEL);
        String frequency = entry.column(VortexKeys.FREQUENCY);
        String sortKeys = entry.column(VortexKeys.SORT_KEYS);
        String nullablekeys = entry.column(VortexKeys.NULLABLE_KEYS);
        String properties = entry.column(VortexKeys.PROPERTIES);
        String indexLabels = entry.column(VortexKeys.INDEX_LABELS);
        String enableLabelIndex = entry.column(VortexKeys.ENABLE_LABEL_INDEX);
        String status = entry.column(VortexKeys.STATUS);
        String ttl = entry.column(VortexKeys.TTL);
        String ttlStartTime = entry.column(VortexKeys.TTL_START_TIME);

        EdgeLabel edgeLabel = new EdgeLabel(graph, id, name);
        edgeLabel.sourceLabel(readId(sourceLabel));
        edgeLabel.targetLabel(readId(targetLabel));
        edgeLabel.frequency(JsonUtil.fromJson(frequency, Frequency.class));
        edgeLabel.properties(readIds(properties));
        edgeLabel.sortKeys(readIds(sortKeys));
        edgeLabel.nullableKeys(readIds(nullablekeys));
        edgeLabel.indexLabels(readIds(indexLabels));
        edgeLabel.enableLabelIndex(JsonUtil.fromJson(enableLabelIndex,
                                                     Boolean.class));
        readUserdata(edgeLabel, entry);
        edgeLabel.status(JsonUtil.fromJson(status, SchemaStatus.class));
        edgeLabel.ttl(JsonUtil.fromJson(ttl, Long.class));
        edgeLabel.ttlStartTime(readId(ttlStartTime));
        return edgeLabel;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        TextBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(VortexKeys.NAME, JsonUtil.toJson(propertyKey.name()));
        entry.column(VortexKeys.DATA_TYPE,
                     JsonUtil.toJson(propertyKey.dataType()));
        entry.column(VortexKeys.CARDINALITY,
                     JsonUtil.toJson(propertyKey.cardinality()));
        entry.column(VortexKeys.AGGREGATE_TYPE,
                     JsonUtil.toJson(propertyKey.aggregateType()));
        entry.column(VortexKeys.WRITE_TYPE,
                     JsonUtil.toJson(propertyKey.writeType()));
        entry.column(VortexKeys.PROPERTIES, writeIds(propertyKey.properties()));
        writeUserdata(propertyKey, entry);
        entry.column(VortexKeys.STATUS,
                     JsonUtil.toJson(propertyKey.status()));
        return entry;
    }

    @Override
    public PropertyKey readPropertyKey(Vortex graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        Id id = readId(entry.id());
        String name = JsonUtil.fromJson(entry.column(VortexKeys.NAME),
                                        String.class);
        String dataType = entry.column(VortexKeys.DATA_TYPE);
        String cardinality = entry.column(VortexKeys.CARDINALITY);
        String aggregateType = entry.column(VortexKeys.AGGREGATE_TYPE);
        String writeType = entry.column(VortexKeys.WRITE_TYPE);
        String properties = entry.column(VortexKeys.PROPERTIES);
        String status = entry.column(VortexKeys.STATUS);

        PropertyKey propertyKey = new PropertyKey(graph, id, name);
        propertyKey.dataType(JsonUtil.fromJson(dataType, DataType.class));
        propertyKey.cardinality(JsonUtil.fromJson(cardinality,
                                                  Cardinality.class));
        propertyKey.aggregateType(JsonUtil.fromJson(aggregateType,
                                                    AggregateType.class));
        propertyKey.writeType(JsonUtil.fromJson(writeType,
                                                WriteType.class));
        propertyKey.properties(readIds(properties));
        readUserdata(propertyKey, entry);
        propertyKey.status(JsonUtil.fromJson(status, SchemaStatus.class));
        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        TextBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(VortexKeys.NAME, JsonUtil.toJson(indexLabel.name()));
        entry.column(VortexKeys.BASE_TYPE,
                     JsonUtil.toJson(indexLabel.baseType()));
        entry.column(VortexKeys.BASE_VALUE, writeId(indexLabel.baseValue()));
        entry.column(VortexKeys.INDEX_TYPE,
                     JsonUtil.toJson(indexLabel.indexType()));
        entry.column(VortexKeys.FIELDS, writeIds(indexLabel.indexFields()));
        writeUserdata(indexLabel, entry);
        entry.column(VortexKeys.STATUS,
                     JsonUtil.toJson(indexLabel.status()));
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(Vortex graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TextBackendEntry entry = this.convertEntry(backendEntry);
        Id id = readId(entry.id());
        String name = JsonUtil.fromJson(entry.column(VortexKeys.NAME),
                                        String.class);
        String baseType = entry.column(VortexKeys.BASE_TYPE);
        String baseValue = entry.column(VortexKeys.BASE_VALUE);
        String indexType = entry.column(VortexKeys.INDEX_TYPE);
        String indexFields = entry.column(VortexKeys.FIELDS);
        String status = entry.column(VortexKeys.STATUS);

        IndexLabel indexLabel = new IndexLabel(graph, id, name);
        indexLabel.baseType(JsonUtil.fromJson(baseType, VortexType.class));
        indexLabel.baseValue(readId(baseValue));
        indexLabel.indexType(JsonUtil.fromJson(indexType, IndexType.class));
        indexLabel.indexFields(readIds(indexFields));
        readUserdata(indexLabel, entry);
        indexLabel.status(JsonUtil.fromJson(status, SchemaStatus.class));
        return indexLabel;
    }

    private String writeEdgeId(Id id, boolean withOwnerVertex) {
        EdgeId edgeId;
        if (id instanceof EdgeId) {
            edgeId = (EdgeId) id;
        } else {
            edgeId = EdgeId.parse(id.asString());
        }
        List<String> list = new ArrayList<>(5);
        if (withOwnerVertex) {
            list.add(writeEntryId(edgeId.ownerVertexId()));
        }
        // Edge name: type + edge-label-name + sortKeys + targetVertex
        list.add(writeType(edgeId.direction().type()));
        list.add(writeId(edgeId.edgeLabelId()));
        list.add(writeEdgeName(edgeId.sortValues()));
        list.add(writeEntryId(edgeId.otherVertexId()));

        return EdgeId.concat(list.toArray(new String[0]));
    }

    private static String writeType(VortexType type) {
        return type.string();
    }

    private static String writeEntryId(Id id) {
        return IdUtil.writeString(id);
    }

    private static Id readEntryId(String id) {
        return IdUtil.readString(id);
    }

    private static String writeEdgeName(String name) {
        return name + EDGE_NAME_ENDING;
    }

    private static String readEdgeName(String name) {
        E.checkState(name.endsWith(EDGE_NAME_ENDING),
                     "Invalid edge name: %s", name);
        return name.substring(0, name.length() - 1);
    }

    private static String writeId(Id id) {
        if (id.number()) {
            return JsonUtil.toJson(id.asLong());
        } else {
            return JsonUtil.toJson(id.asString());
        }
    }

    private static Id readId(String id) {
        Object value = JsonUtil.fromJson(id, Object.class);
        if (value instanceof Number) {
            return IdGenerator.of(((Number) value).longValue());
        } else {
            assert value instanceof String;
            return IdGenerator.of(value.toString());
        }
    }

    private static Id readId(Id id) {
        return readId(id.asString());
    }

    private static String writeIds(Collection<Id> ids) {
        Object[] array = new Object[ids.size()];
        int i = 0;
        for (Id id : ids) {
            if (id.number()) {
                array[i++] = id.asLong();
            } else {
                array[i++] = id.asString();
            }
        }
        return JsonUtil.toJson(array);
    }

    private static Id[] readIds(String str) {
        Object[] values = JsonUtil.fromJson(str, Object[].class);
        Id[] ids = new Id[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value instanceof Number) {
                ids[i] = IdGenerator.of(((Number) value).longValue());
            } else {
                assert value instanceof String;
                ids[i] = IdGenerator.of(value.toString());
            }
        }
        return ids;
    }

    private static String writeElementId(Id id, long expiredTime) {
        Object[] array = new Object[1];
        Object idValue = id.number() ? id.asLong() : id.asString();
        if (expiredTime <= 0L) {
            array[0] = id;
        } else {
            array[0] = ImmutableMap.of(VortexKeys.ID.string(), idValue,
                                       VortexKeys.EXPIRED_TIME.string(),
                                       expiredTime);
        }
        return JsonUtil.toJson(array);
    }

    private static IdWithExpiredTime[] readElementIds(String str) {
        Object[] values = JsonUtil.fromJson(str, Object[].class);
        IdWithExpiredTime[] ids = new IdWithExpiredTime[values.length];
        for (int i = 0; i < values.length; i++) {
            Object idValue;
            long expiredTime;
            if (values[i] instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) values[i];
                idValue = map.get(VortexKeys.ID.string());
                expiredTime = ((Number) map.get(
                              VortexKeys.EXPIRED_TIME.string())).longValue();
            } else {
                idValue = values[i];
                expiredTime = 0L;
            }
            Id id;
            if (idValue instanceof Number) {
                id = IdGenerator.of(((Number) idValue).longValue());
            } else {
                assert idValue instanceof String;
                id = IdGenerator.of(idValue.toString());
            }
            ids[i] = new IdWithExpiredTime(id, expiredTime);
        }
        return ids;
    }

    private static String writeLong(long value) {
        return JsonUtil.toJson(value);
    }

    private static long readLong(String value) {
        return Long.parseLong(value);
    }

    private static void writeUserdata(SchemaElement schema,
                                      TextBackendEntry entry) {
        entry.column(VortexKeys.USER_DATA, JsonUtil.toJson(schema.userdata()));
    }

    private static void readUserdata(SchemaElement schema,
                                     TextBackendEntry entry) {
        // Parse all user data of a schema element
        String userdataStr = entry.column(VortexKeys.USER_DATA);
        @SuppressWarnings("unchecked")
        Map<String, Object> userdata = JsonUtil.fromJson(userdataStr,
                                                         Map.class);
        for (Map.Entry<String, Object> e : userdata.entrySet()) {
            schema.userdata(e.getKey(), e.getValue());
        }
    }
}
