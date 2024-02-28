package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.EdgeId;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.page.PageState;
import com.vortex.vortexdb.backend.query.*;
import com.vortex.vortexdb.backend.query.Condition.RangeConditions;
import com.vortex.vortexdb.backend.serializer.BinaryBackendEntry.BinaryId;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendEntry.BackendColumn;
import com.vortex.vortexdb.schema.*;
import com.vortex.vortexdb.structure.*;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.*;
import com.vortex.common.util.*;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.vortexdb.util.StringEncoding;
import org.apache.commons.lang.NotImplementedException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BinarySerializer extends AbstractSerializer {

    public static final byte[] EMPTY_BYTES = new byte[0];

    /*
     * Id is stored in column name if keyWithIdPrefix=true like RocksDB,
     * else stored in rowkey like HBase.
     */
    private final boolean keyWithIdPrefix;
    private final boolean indexWithIdPrefix;

    public BinarySerializer() {
        this(true, true);
    }

    public BinarySerializer(boolean keyWithIdPrefix,
                            boolean indexWithIdPrefix) {
        this.keyWithIdPrefix = keyWithIdPrefix;
        this.indexWithIdPrefix = indexWithIdPrefix;
    }

    @Override
    protected BinaryBackendEntry newBackendEntry(VortexType type, Id id) {
        if (type.isEdge()) {
            E.checkState(id instanceof BinaryId,
                         "Expect a BinaryId for BackendEntry with edge id");
            return new BinaryBackendEntry(type, (BinaryId) id);
        }

        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        byte[] idBytes = type.isIndex() ?
                         buffer.writeIndexId(id, type).bytes() :
                         buffer.writeId(id).bytes();
        return new BinaryBackendEntry(type, new BinaryId(idBytes, id));
    }

    protected final BinaryBackendEntry newBackendEntry(VortexVertex vertex) {
        return newBackendEntry(vertex.type(), vertex.id());
    }

    protected final BinaryBackendEntry newBackendEntry(VortexEdge edge) {
        BinaryId id = new BinaryId(formatEdgeName(edge),
                                   edge.idWithDirection());
        return newBackendEntry(edge.type(), id);
    }

    protected final BinaryBackendEntry newBackendEntry(SchemaElement elem) {
        return newBackendEntry(elem.type(), elem.id());
    }

    @Override
    protected BinaryBackendEntry convertEntry(BackendEntry entry) {
        assert entry instanceof BinaryBackendEntry;
        return (BinaryBackendEntry) entry;
    }

    protected byte[] formatSyspropName(Id id, VortexKeys col) {
        int idLen = this.keyWithIdPrefix ? 1 + id.length() : 0;
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 1 + 1);
        byte sysprop = VortexType.SYS_PROPERTY.code();
        if (this.keyWithIdPrefix) {
            buffer.writeId(id);
        }
        return buffer.write(sysprop).write(col.code()).bytes();
    }

    protected byte[] formatSyspropName(BinaryId id, VortexKeys col) {
        int idLen = this.keyWithIdPrefix ? id.length() : 0;
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 1 + 1);
        byte sysprop = VortexType.SYS_PROPERTY.code();
        if (this.keyWithIdPrefix) {
            buffer.write(id.asBytes());
        }
        return buffer.write(sysprop).write(col.code()).bytes();
    }

    protected BackendColumn formatLabel(VortexElement elem) {
        BackendColumn col = new BackendColumn();
        col.name = this.formatSyspropName(elem.id(), VortexKeys.LABEL);
        Id label = elem.schemaLabel().id();
        BytesBuffer buffer = BytesBuffer.allocate(label.length() + 1);
        col.value = buffer.writeId(label).bytes();
        return col;
    }

    protected byte[] formatPropertyName(VortexProperty<?> prop) {
        Id id = prop.element().id();
        int idLen = this.keyWithIdPrefix ? 1 + id.length() : 0;
        Id pkeyId = prop.propertyKey().id();
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 2 + pkeyId.length());
        if (this.keyWithIdPrefix) {
            buffer.writeId(id);
        }
        buffer.write(prop.type().code());
        buffer.writeId(pkeyId);
        return buffer.bytes();
    }

    protected BackendColumn formatProperty(VortexProperty<?> prop) {
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_PROPERTY);
        buffer.writeProperty(prop.propertyKey(), prop.value());
        return BackendColumn.of(this.formatPropertyName(prop), buffer.bytes());
    }

    protected void parseProperty(Id pkeyId, BytesBuffer buffer,
                                 VortexElement owner) {
        PropertyKey pkey = owner.graph().propertyKey(pkeyId);

        // Parse value
        Object value = buffer.readProperty(pkey);

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

    protected void formatProperties(Collection<VortexProperty<?>> props,
                                    BytesBuffer buffer) {
        // Write properties size
        buffer.writeVInt(props.size());

        // Write properties data
        for (VortexProperty<?> property : props) {
            PropertyKey pkey = property.propertyKey();
            buffer.writeVInt(SchemaElement.schemaId(pkey.id()));
            buffer.writeProperty(pkey, property.value());
        }
    }

    protected void parseProperties(BytesBuffer buffer, VortexElement owner) {
        int size = buffer.readVInt();
        assert size >= 0;
        for (int i = 0; i < size; i++) {
            Id pkeyId = IdGenerator.of(buffer.readVInt());
            this.parseProperty(pkeyId, buffer, owner);
        }
    }

    protected void formatExpiredTime(long expiredTime, BytesBuffer buffer) {
        buffer.writeVLong(expiredTime);
    }

    protected void parseExpiredTime(BytesBuffer buffer, VortexElement element) {
        element.expiredTime(buffer.readVLong());
    }

    protected byte[] formatEdgeName(VortexEdge edge) {
        // owner-vertex + dir + edge-label + sort-values + other-vertex
        return BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID)
                          .writeEdgeId(edge.id()).bytes();
    }

    protected byte[] formatEdgeValue(VortexEdge edge) {
        int propsCount = edge.sizeOfProperties();
        BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * propsCount);

        // Write edge id
        //buffer.writeId(edge.id());

        // Write edge properties
        this.formatProperties(edge.getProperties(), buffer);

        // Write edge expired time if needed
        if (edge.hasTtl()) {
            this.formatExpiredTime(edge.expiredTime(), buffer);
        }

        return buffer.bytes();
    }

    protected void parseEdge(BackendColumn col, VortexVertex vertex,
                             Vortex graph) {
        // owner-vertex + dir + edge-label + sort-values + other-vertex

        BytesBuffer buffer = BytesBuffer.wrap(col.name);
        if (this.keyWithIdPrefix) {
            // Consume owner-vertex id
            buffer.readId();
        }
        byte type = buffer.read();
        Id labelId = buffer.readId();
        String sortValues = buffer.readStringWithEnding();
        Id otherVertexId = buffer.readId();

        boolean direction = EdgeId.isOutDirectionFromCode(type);
        EdgeLabel edgeLabel = graph.edgeLabelOrNone(labelId);

        // Construct edge
        VortexEdge edge = VortexEdge.constructEdge(vertex, direction, edgeLabel,
                                               sortValues, otherVertexId);

        // Parse edge-id + edge-properties
        buffer = BytesBuffer.wrap(col.value);

        //Id id = buffer.readId();

        // Parse edge properties
        this.parseProperties(buffer, edge);

        // Parse edge expired time if needed
        if (edge.hasTtl()) {
            this.parseExpiredTime(buffer, edge);
        }
    }


    protected void parseVertex(byte[] value, VortexVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(value);

        // Parse vertex label
        VertexLabel label = vertex.graph().vertexLabelOrNone(buffer.readId());
        vertex.correctVertexLabel(label);

        // Parse properties
        this.parseProperties(buffer, vertex);

        // Parse vertex expired time if needed
        if (vertex.hasTtl()) {
            this.parseExpiredTime(buffer, vertex);
        }
    }

    protected void parseColumn(BackendColumn col, VortexVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(col.name);
        Id id = this.keyWithIdPrefix ? buffer.readId() : vertex.id();
        E.checkState(buffer.remaining() > 0, "Missing column type");
        byte type = buffer.read();
        // Parse property
        if (type == VortexType.PROPERTY.code()) {
            Id pkeyId = buffer.readId();
            this.parseProperty(pkeyId, BytesBuffer.wrap(col.value), vertex);
        }
        // Parse edge
        else if (type == VortexType.EDGE_IN.code() ||
                 type == VortexType.EDGE_OUT.code()) {
            this.parseEdge(col, vertex, vertex.graph());
        }
        // Parse system property
        else if (type == VortexType.SYS_PROPERTY.code()) {
            // pass
        }
        // Invalid entry
        else {
            E.checkState(false, "Invalid entry(%s) with unknown type(%s): 0x%s",
                         id, type & 0xff, Bytes.toHex(col.name));
        }
    }

    protected byte[] formatIndexName(VortexIndex index) {
        BytesBuffer buffer;
        Id elemId = index.elementId();
        if (!this.indexWithIdPrefix) {
            int idLen = 1 + elemId.length();
            buffer = BytesBuffer.allocate(idLen);
        } else {
            Id indexId = index.id();
            VortexType type = index.type();
            if (!type.isNumericIndex() && indexIdLengthExceedLimit(indexId)) {
                indexId = index.hashId();
            }
            int idLen = 1 + elemId.length() + 1 + indexId.length();
            buffer = BytesBuffer.allocate(idLen);
            // Write index-id
            buffer.writeIndexId(indexId, type);
        }
        // Write element-id
        buffer.writeId(elemId);
        // Write expired time if needed
        if (index.hasTtl()) {
            buffer.writeVLong(index.expiredTime());
        }

        return buffer.bytes();
    }

    protected void parseIndexName(Vortex graph, ConditionQuery query,
                                  BinaryBackendEntry entry,
                                  VortexIndex index, Object fieldValues) {
        for (BackendColumn col : entry.columns()) {
            if (indexFieldValuesUnmatched(col.value, fieldValues)) {
                // Skip if field-values is not matched (just the same hash)
                continue;
            }
            BytesBuffer buffer = BytesBuffer.wrap(col.name);
            if (this.indexWithIdPrefix) {
                buffer.readIndexId(index.type());
            }
            Id elemId = buffer.readId();
            long expiredTime = index.hasTtl() ? buffer.readVLong() : 0L;
            index.elementIds(elemId, expiredTime);
        }
    }

    @Override
    public BackendEntry writeVertex(VortexVertex vertex) {
        if (vertex.olap()) {
            return this.writeOlapVertex(vertex);
        }

        BinaryBackendEntry entry = newBackendEntry(vertex);

        if (vertex.removed()) {
            return entry;
        }

        int propsCount = vertex.sizeOfProperties();
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);

        // Write vertex label
        buffer.writeId(vertex.schemaLabel().id());

        // Write all properties of the vertex
        this.formatProperties(vertex.getProperties(), buffer);

        // Write vertex expired time if needed
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
            this.formatExpiredTime(vertex.expiredTime(), buffer);
        }

        // Fill column
        byte[] name = this.keyWithIdPrefix ? entry.id().asBytes() : EMPTY_BYTES;
        entry.column(name, buffer.bytes());

        return entry;
    }

    @Override
    public BackendEntry writeOlapVertex(VortexVertex vertex) {
        BinaryBackendEntry entry = newBackendEntry(VortexType.OLAP, vertex.id());
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16);

        Collection<VortexProperty<?>> properties = vertex.getProperties();
        E.checkArgument(properties.size() == 1,
                        "Expect only 1 property for olap vertex, but got %s",
                        properties.size());
        VortexProperty<?> property = properties.iterator().next();
        PropertyKey propertyKey = property.propertyKey();
        buffer.writeVInt(SchemaElement.schemaId(propertyKey.id()));
        buffer.writeProperty(propertyKey, property.value());

        // Fill column
        byte[] name = this.keyWithIdPrefix ? entry.id().asBytes() : EMPTY_BYTES;
        entry.column(name, buffer.bytes());
        entry.subId(propertyKey.id());
        entry.olap(true);
        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(VortexVertexProperty<?> prop) {
        throw new NotImplementedException("Unsupported writeVertexProperty()");
    }

    @Override
    public VortexVertex readVertex(Vortex graph, BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);

        // Parse id
        Id id = entry.id().origin();
        Id vid = id.edge() ? ((EdgeId) id).ownerVertexId() : id;
        VortexVertex vertex = new VortexVertex(graph, vid, VertexLabel.NONE);

        // Parse all properties and edges of a Vertex
        Iterator<BackendColumn> iterator = entry.columns().iterator();
        for (int index = 0; iterator.hasNext(); index++) {
            BackendColumn col = iterator.next();
            if (entry.type().isEdge()) {
                // NOTE: the entry id type is vertex even if entry type is edge
                // Parse vertex edges
                this.parseColumn(col, vertex);
            } else {
                assert entry.type().isVertex();
                // Parse vertex properties
                assert entry.columnsSize() >= 1 : entry.columnsSize();
                if (index == 0) {
                    this.parseVertex(col.value, vertex);
                } else {
                    this.parseVertexOlap(col.value, vertex);
                }
            }
        }

        return vertex;
    }

    protected void parseVertexOlap(byte[] value, VortexVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(value);
        Id pkeyId = IdGenerator.of(buffer.readVInt());
        this.parseProperty(pkeyId, buffer, vertex);
    }

    @Override
    public BackendEntry writeEdge(VortexEdge edge) {
        BinaryBackendEntry entry = newBackendEntry(edge);
        byte[] name = this.keyWithIdPrefix ?
                      this.formatEdgeName(edge) : EMPTY_BYTES;
        byte[] value = this.formatEdgeValue(edge);
        entry.column(name, value);

        if (edge.hasTtl()) {
            entry.ttl(edge.ttl());
        }

        return entry;
    }

    @Override
    public BackendEntry writeEdgeProperty(VortexEdgeProperty<?> prop) {
        // TODO: entry.column(this.formatProperty(prop));
        throw new NotImplementedException("Unsupported writeEdgeProperty()");
    }

    @Override
    public VortexEdge readEdge(Vortex graph, BackendEntry bytesEntry) {
        VortexVertex vertex = this.readVertex(graph, bytesEntry);
        Collection<VortexEdge> edges = vertex.getEdges();
        E.checkState(edges.size() == 1,
                     "Expect one edge in vertex, but got %s", edges.size());
        return edges.iterator().next();
    }

    @Override
    public BackendEntry writeIndex(VortexIndex index) {
        BinaryBackendEntry entry;
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            /*
             * When field-values is null and elementIds size is 0, it is
             * meaningful for deletion of index data by index label.
             * TODO: improve
             */
            entry = this.formatILDeletion(index);
        } else {
            Id id = index.id();
            VortexType type = index.type();
            byte[] value = null;
            if (!type.isNumericIndex() && indexIdLengthExceedLimit(id)) {
                id = index.hashId();
                // Save field-values as column value if the key is a hash string
                value = StringEncoding.encode(index.fieldValues().toString());
            }

            entry = newBackendEntry(type, id);
            if (index.indexLabel().olap()) {
                entry.olap(true);
            }
            entry.column(this.formatIndexName(index), value);
            entry.subId(index.elementId());

            if (index.hasTtl()) {
                entry.ttl(index.ttl());
            }
        }
        return entry;
    }

    @Override
    public VortexIndex readIndex(Vortex graph, ConditionQuery query,
                               BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }

        BinaryBackendEntry entry = this.convertEntry(bytesEntry);
        // NOTE: index id without length prefix
        byte[] bytes = entry.id().asBytes();
        VortexIndex index = VortexIndex.parseIndexId(graph, entry.type(), bytes);

        Object fieldValues = null;
        if (!index.type().isRangeIndex()) {
            fieldValues = query.condition(VortexKeys.FIELD_VALUES);
            if (!index.fieldValues().equals(fieldValues)) {
                // Update field-values for hashed or encoded index-id
                index.fieldValues(fieldValues);
            }
        }

        this.parseIndexName(graph, query, entry, index, fieldValues);
        return index;
    }

    @Override
    public BackendEntry writeId(VortexType type, Id id) {
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(VortexType type, Id id) {
        if (type.isEdge()) {
            id = writeEdgeId(id);
        } else {
            BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
            id = new BinaryId(buffer.writeId(id).bytes(), id);
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
        Id vertex = cq.condition(VortexKeys.OWNER_VERTEX);
        Directions direction = cq.condition(VortexKeys.DIRECTION);
        if (direction == null) {
            direction = Directions.OUT;
        }
        Id label = cq.condition(VortexKeys.LABEL);

        int size = 1 + vertex.length() + 1 + label.length() + 16;
        BytesBuffer start = BytesBuffer.allocate(size);
        start.writeId(vertex);
        start.write(direction.type().code());
        start.writeId(label);

        BytesBuffer end = BytesBuffer.allocate(size);
        end.copyFrom(start);

        RangeConditions range = new RangeConditions(sortValues);
        if (range.keyMin() != null) {
            start.writeStringRaw((String) range.keyMin());
        }
        if (range.keyMax() != null) {
            end.writeStringRaw((String) range.keyMax());
        }
        // Sort-value will be empty if there is no start sort-value
        Id startId = new BinaryId(start.bytes(), null);
        // Set endId as prefix if there is no end sort-value
        Id endId = new BinaryId(end.bytes(), null);

        boolean includeStart = range.keyMinEq();
        if (cq.paging() && !cq.page().isEmpty()) {
            includeStart = true;
            byte[] position = PageState.fromString(cq.page()).position();
            E.checkArgument(Bytes.compare(position, startId.asBytes()) >= 0,
                            "Invalid page out of lower bound");
            startId = new BinaryId(position, null);
        }
        if (range.keyMax() == null) {
            return new IdPrefixQuery(cq, startId, includeStart, endId);
        }
        return new IdRangeQuery(cq, startId, includeStart, endId,
                                range.keyMaxEq());
    }

    private Query writeQueryEdgePrefixCondition(ConditionQuery cq) {
        int count = 0;
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
        for (VortexKeys key : EdgeId.KEYS) {
            Object value = cq.condition(key);

            if (value != null) {
                count++;
            } else {
                if (key == VortexKeys.DIRECTION) {
                    // Direction is null, set to OUT
                    value = Directions.OUT;
                } else {
                    break;
                }
            }

            if (key == VortexKeys.OWNER_VERTEX ||
                key == VortexKeys.OTHER_VERTEX) {
                buffer.writeId((Id) value);
            } else if (key == VortexKeys.DIRECTION) {
                byte t = ((Directions) value).type().code();
                buffer.write(t);
            } else if (key == VortexKeys.LABEL) {
                assert value instanceof Id;
                buffer.writeId((Id) value);
            } else if (key == VortexKeys.SORT_VALUES) {
                assert value instanceof String;
                buffer.writeStringWithEnding((String) value);
            } else {
                assert false : key;
            }
        }

        if (count > 0) {
            assert count == cq.conditionsSize();
            return prefixQuery(cq, new BinaryId(buffer.bytes(), null));
        }

        return null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        VortexType type = query.resultType();
        if (!type.isIndex()) {
            return query;
        }

        ConditionQuery cq = (ConditionQuery) query;

        if (type.isNumericIndex()) {
            // Convert range-index/shard-index query to id range query
            return this.writeRangeIndexQuery(cq);
        } else {
            assert type.isSearchIndex() || type.isSecondaryIndex() ||
                   type.isUniqueIndex();
            // Convert secondary-index or search-index query to id query
            return this.writeStringIndexQuery(cq);
        }
    }

    private Query writeStringIndexQuery(ConditionQuery query) {
        E.checkArgument(query.allSysprop() &&
                        query.conditionsSize() == 2,
                        "There should be two conditions: " +
                        "INDEX_LABEL_ID and FIELD_VALUES" +
                        "in secondary index query");

        Id index = query.condition(VortexKeys.INDEX_LABEL_ID);
        Object key = query.condition(VortexKeys.FIELD_VALUES);

        E.checkArgument(index != null, "Please specify the index label");
        E.checkArgument(key != null, "Please specify the index key");

        Id prefix = formatIndexId(query.resultType(), index, key, true);
        return prefixQuery(query, prefix);
    }

    private Query writeRangeIndexQuery(ConditionQuery query) {
        Id index = query.condition(VortexKeys.INDEX_LABEL_ID);
        E.checkArgument(index != null, "Please specify the index label");

        List<Condition> fields = query.syspropConditions(VortexKeys.FIELD_VALUES);
        E.checkArgument(!fields.isEmpty(),
                        "Please specify the index field values");

        VortexType type = query.resultType();
        Id start = null;
        if (query.paging() && !query.page().isEmpty()) {
            byte[] position = PageState.fromString(query.page()).position();
            start = new BinaryId(position, null);
        }

        RangeConditions range = new RangeConditions(fields);
        if (range.keyEq() != null) {
            Id id = formatIndexId(type, index, range.keyEq(), true);
            if (start == null) {
                return new IdPrefixQuery(query, id);
            }
            E.checkArgument(Bytes.compare(start.asBytes(), id.asBytes()) >= 0,
                            "Invalid page out of lower bound");
            return new IdPrefixQuery(query, start, id);
        }

        Object keyMin = range.keyMin();
        Object keyMax = range.keyMax();
        boolean keyMinEq = range.keyMinEq();
        boolean keyMaxEq = range.keyMaxEq();
        if (keyMin == null) {
            E.checkArgument(keyMax != null,
                            "Please specify at least one condition");
            // Set keyMin to min value
            keyMin = NumericUtil.minValueOf(keyMax.getClass());
            keyMinEq = true;
        }

        Id min = formatIndexId(type, index, keyMin, false);
        if (!keyMinEq) {
            /*
             * Increase 1 to keyMin, index GT query is a scan with GT prefix,
             * inclusiveStart=false will also match index started with keyMin
             */
            increaseOne(min.asBytes());
            keyMinEq = true;
        }

        if (start == null) {
            start = min;
        } else {
            E.checkArgument(Bytes.compare(start.asBytes(), min.asBytes()) >= 0,
                            "Invalid page out of lower bound");
        }

        if (keyMax == null) {
            keyMax = NumericUtil.maxValueOf(keyMin.getClass());
            keyMaxEq = true;
        }
        Id max = formatIndexId(type, index, keyMax, false);
        if (keyMaxEq) {
            keyMaxEq = false;
            increaseOne(max.asBytes());
        }
        return new IdRangeQuery(query, start, keyMinEq, max, keyMaxEq);
    }

    private BinaryBackendEntry formatILDeletion(VortexIndex index) {
        Id id = index.indexLabelId();
        BinaryId bid = new BinaryId(id.asBytes(), id);
        BinaryBackendEntry entry = new BinaryBackendEntry(index.type(), bid);
        if (index.type().isStringIndex()) {
            byte[] idBytes = IdGenerator.of(id.asString()).asBytes();
            BytesBuffer buffer = BytesBuffer.allocate(idBytes.length);
            buffer.write(idBytes);
            entry.column(buffer.bytes(), null);
        } else {
            assert index.type().isRangeIndex();
            BytesBuffer buffer = BytesBuffer.allocate(4);
            buffer.writeInt((int) id.asLong());
            entry.column(buffer.bytes(), null);
        }
        return entry;
    }

    private static BinaryId writeEdgeId(Id id) {
        EdgeId edgeId;
        if (id instanceof EdgeId) {
            edgeId = (EdgeId) id;
        } else {
            edgeId = EdgeId.parse(id.asString());
        }
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID)
                                        .writeEdgeId(edgeId);
        return new BinaryId(buffer.bytes(), id);
    }

    private static Query prefixQuery(ConditionQuery query, Id prefix) {
        Query newQuery;
        if (query.paging() && !query.page().isEmpty()) {
            /*
             * If used paging and the page number is not empty, deserialize
             * the page to id and use it as the starting row for this query
             */
            byte[] position = PageState.fromString(query.page()).position();
            E.checkArgument(Bytes.compare(position, prefix.asBytes()) >= 0,
                            "Invalid page out of lower bound");
            BinaryId start = new BinaryId(position, null);
            newQuery = new IdPrefixQuery(query, start, prefix);
        } else {
            newQuery = new IdPrefixQuery(query, prefix);
        }
        return newQuery;
    }

    protected static BinaryId formatIndexId(VortexType type, Id indexLabel,
                                            Object fieldValues,
                                            boolean equal) {
        boolean withEnding = type.isRangeIndex() || equal;
        Id id = VortexIndex.formatIndexId(type, indexLabel, fieldValues);
        if (!type.isNumericIndex() && indexIdLengthExceedLimit(id)) {
            id = VortexIndex.formatIndexHashId(type, indexLabel, fieldValues);
        }
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        byte[] idBytes = buffer.writeIndexId(id, type, withEnding).bytes();
        return new BinaryId(idBytes, id);
    }

    protected static boolean indexIdLengthExceedLimit(Id id) {
        return id.asBytes().length > BytesBuffer.INDEX_HASH_ID_THRESHOLD;
    }

    protected static boolean indexFieldValuesUnmatched(byte[] value,
                                                       Object fieldValues) {
        if (value != null && value.length > 0 && fieldValues != null) {
            if (!StringEncoding.decode(value).equals(fieldValues)) {
                return true;
            }
        }
        return false;
    }

    public static final byte[] increaseOne(byte[] bytes) {
        final byte BYTE_MAX_VALUE = (byte) 0xff;
        assert bytes.length > 0;
        byte last = bytes[bytes.length - 1];
        if (last != BYTE_MAX_VALUE) {
            bytes[bytes.length - 1] += 0x01;
        } else {
            // Process overflow (like [1, 255] => [2, 0])
            int i = bytes.length - 1;
            for (; i > 0 && bytes[i] == BYTE_MAX_VALUE; --i) {
                bytes[i] += 0x01;
            }
            if (bytes[i] == BYTE_MAX_VALUE) {
                assert i == 0;
                throw new BackendException("Unable to increase bytes: %s",
                                           Bytes.toHex(bytes));
            }
            bytes[i] += 0x01;
        }
        return bytes;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeVertexLabel(vertexLabel);
    }

    @Override
    public VertexLabel readVertexLabel(Vortex graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readVertexLabel(graph, entry);
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeEdgeLabel(edgeLabel);
    }

    @Override
    public EdgeLabel readEdgeLabel(Vortex graph, BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readEdgeLabel(graph, entry);
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writePropertyKey(propertyKey);
    }

    @Override
    public PropertyKey readPropertyKey(Vortex graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readPropertyKey(graph, entry);
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeIndexLabel(indexLabel);
    }

    @Override
    public IndexLabel readIndexLabel(Vortex graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readIndexLabel(graph, entry);
    }

    private final class SchemaSerializer {

        private BinaryBackendEntry entry;

        public BinaryBackendEntry writeVertexLabel(VertexLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(VortexKeys.NAME, schema.name());
            writeEnum(VortexKeys.ID_STRATEGY, schema.idStrategy());
            writeIds(VortexKeys.PROPERTIES, schema.properties());
            writeIds(VortexKeys.PRIMARY_KEYS, schema.primaryKeys());
            writeIds(VortexKeys.NULLABLE_KEYS, schema.nullableKeys());
            writeIds(VortexKeys.INDEX_LABELS, schema.indexLabels());
            writeBool(VortexKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
            writeEnum(VortexKeys.STATUS, schema.status());
            writeLong(VortexKeys.TTL, schema.ttl());
            writeId(VortexKeys.TTL_START_TIME, schema.ttlStartTime());
            writeUserdata(schema);
            return this.entry;
        }

        public VertexLabel readVertexLabel(Vortex graph,
                                           BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(VortexKeys.NAME);

            VertexLabel vertexLabel = new VertexLabel(graph, id, name);
            vertexLabel.idStrategy(readEnum(VortexKeys.ID_STRATEGY,
                                            IdStrategy.class));
            vertexLabel.properties(readIds(VortexKeys.PROPERTIES));
            vertexLabel.primaryKeys(readIds(VortexKeys.PRIMARY_KEYS));
            vertexLabel.nullableKeys(readIds(VortexKeys.NULLABLE_KEYS));
            vertexLabel.indexLabels(readIds(VortexKeys.INDEX_LABELS));
            vertexLabel.enableLabelIndex(readBool(VortexKeys.ENABLE_LABEL_INDEX));
            vertexLabel.status(readEnum(VortexKeys.STATUS, SchemaStatus.class));
            vertexLabel.ttl(readLong(VortexKeys.TTL));
            vertexLabel.ttlStartTime(readId(VortexKeys.TTL_START_TIME));
            readUserdata(vertexLabel);
            return vertexLabel;
        }

        public BinaryBackendEntry writeEdgeLabel(EdgeLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(VortexKeys.NAME, schema.name());
            writeId(VortexKeys.SOURCE_LABEL, schema.sourceLabel());
            writeId(VortexKeys.TARGET_LABEL, schema.targetLabel());
            writeEnum(VortexKeys.FREQUENCY, schema.frequency());
            writeIds(VortexKeys.PROPERTIES, schema.properties());
            writeIds(VortexKeys.SORT_KEYS, schema.sortKeys());
            writeIds(VortexKeys.NULLABLE_KEYS, schema.nullableKeys());
            writeIds(VortexKeys.INDEX_LABELS, schema.indexLabels());
            writeBool(VortexKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
            writeEnum(VortexKeys.STATUS, schema.status());
            writeLong(VortexKeys.TTL, schema.ttl());
            writeId(VortexKeys.TTL_START_TIME, schema.ttlStartTime());
            writeUserdata(schema);
            return this.entry;
        }

        public EdgeLabel readEdgeLabel(Vortex graph,
                                       BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(VortexKeys.NAME);

            EdgeLabel edgeLabel = new EdgeLabel(graph, id, name);
            edgeLabel.sourceLabel(readId(VortexKeys.SOURCE_LABEL));
            edgeLabel.targetLabel(readId(VortexKeys.TARGET_LABEL));
            edgeLabel.frequency(readEnum(VortexKeys.FREQUENCY, Frequency.class));
            edgeLabel.properties(readIds(VortexKeys.PROPERTIES));
            edgeLabel.sortKeys(readIds(VortexKeys.SORT_KEYS));
            edgeLabel.nullableKeys(readIds(VortexKeys.NULLABLE_KEYS));
            edgeLabel.indexLabels(readIds(VortexKeys.INDEX_LABELS));
            edgeLabel.enableLabelIndex(readBool(VortexKeys.ENABLE_LABEL_INDEX));
            edgeLabel.status(readEnum(VortexKeys.STATUS, SchemaStatus.class));
            edgeLabel.ttl(readLong(VortexKeys.TTL));
            edgeLabel.ttlStartTime(readId(VortexKeys.TTL_START_TIME));
            readUserdata(edgeLabel);
            return edgeLabel;
        }

        public BinaryBackendEntry writePropertyKey(PropertyKey schema) {
            this.entry = newBackendEntry(schema);
            writeString(VortexKeys.NAME, schema.name());
            writeEnum(VortexKeys.DATA_TYPE, schema.dataType());
            writeEnum(VortexKeys.CARDINALITY, schema.cardinality());
            writeEnum(VortexKeys.AGGREGATE_TYPE, schema.aggregateType());
            writeEnum(VortexKeys.WRITE_TYPE, schema.writeType());
            writeIds(VortexKeys.PROPERTIES, schema.properties());
            writeEnum(VortexKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public PropertyKey readPropertyKey(Vortex graph,
                                           BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(VortexKeys.NAME);

            PropertyKey propertyKey = new PropertyKey(graph, id, name);
            propertyKey.dataType(readEnum(VortexKeys.DATA_TYPE, DataType.class));
            propertyKey.cardinality(readEnum(VortexKeys.CARDINALITY,
                                             Cardinality.class));
            propertyKey.aggregateType(readEnum(VortexKeys.AGGREGATE_TYPE,
                                               AggregateType.class));
            propertyKey.writeType(readEnumOrDefault(VortexKeys.WRITE_TYPE,
                                                    WriteType.class,
                                                    WriteType.OLTP));
            propertyKey.properties(readIds(VortexKeys.PROPERTIES));
            propertyKey.status(readEnum(VortexKeys.STATUS, SchemaStatus.class));
            readUserdata(propertyKey);
            return propertyKey;
        }

        public BinaryBackendEntry writeIndexLabel(IndexLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(VortexKeys.NAME, schema.name());
            writeEnum(VortexKeys.BASE_TYPE, schema.baseType());
            writeId(VortexKeys.BASE_VALUE, schema.baseValue());
            writeEnum(VortexKeys.INDEX_TYPE, schema.indexType());
            writeIds(VortexKeys.FIELDS, schema.indexFields());
            writeEnum(VortexKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public IndexLabel readIndexLabel(Vortex graph,
                                         BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(VortexKeys.NAME);

            IndexLabel indexLabel = new IndexLabel(graph, id, name);
            indexLabel.baseType(readEnum(VortexKeys.BASE_TYPE, VortexType.class));
            indexLabel.baseValue(readId(VortexKeys.BASE_VALUE));
            indexLabel.indexType(readEnum(VortexKeys.INDEX_TYPE,
                                          IndexType.class));
            indexLabel.indexFields(readIds(VortexKeys.FIELDS));
            indexLabel.status(readEnum(VortexKeys.STATUS, SchemaStatus.class));
            readUserdata(indexLabel);
            return indexLabel;
        }

        private void writeUserdata(SchemaElement schema) {
            String userdataStr = JsonUtil.toJson(schema.userdata());
            writeString(VortexKeys.USER_DATA, userdataStr);
        }

        private void readUserdata(SchemaElement schema) {
            // Parse all user data of a schema element
            byte[] userdataBytes = column(VortexKeys.USER_DATA);
            String userdataStr = StringEncoding.decode(userdataBytes);
            @SuppressWarnings("unchecked")
            Map<String, Object> userdata = JsonUtil.fromJson(userdataStr,
                                                             Map.class);
            for (Map.Entry<String, Object> e : userdata.entrySet()) {
                schema.userdata(e.getKey(), e.getValue());
            }
        }

        private void writeString(VortexKeys key, String value) {
            this.entry.column(formatColumnName(key),
                              StringEncoding.encode(value));
        }

        private String readString(VortexKeys key) {
            return StringEncoding.decode(column(key));
        }

        private void writeEnum(VortexKeys key, SerialEnum value) {
            this.entry.column(formatColumnName(key), new byte[]{value.code()});
        }

        private <T extends SerialEnum> T readEnum(VortexKeys key,
                                                  Class<T> clazz) {
            byte[] value = column(key);
            E.checkState(value.length == 1,
                         "The length of column '%s' must be 1, but is '%s'",
                         key, value.length);
            return SerialEnum.fromCode(clazz, value[0]);
        }

        private <T extends SerialEnum> T readEnumOrDefault(VortexKeys key,
                                                           Class<T> clazz,
                                                           T defaultValue) {
            BackendColumn column = this.entry.column(formatColumnName(key));
            if (column == null) {
                return defaultValue;
            }
            E.checkNotNull(column.value, "column.value");
            return SerialEnum.fromCode(clazz, column.value[0]);
        }

        private void writeLong(VortexKeys key, long value) {
            @SuppressWarnings("resource")
            BytesBuffer buffer = new BytesBuffer(8);
            buffer.writeVLong(value);
            this.entry.column(formatColumnName(key), buffer.bytes());
        }

        private long readLong(VortexKeys key) {
            byte[] value = column(key);
            BytesBuffer buffer = BytesBuffer.wrap(value);
            return buffer.readVLong();
        }

        private void writeId(VortexKeys key, Id value) {
            this.entry.column(formatColumnName(key), writeId(value));
        }

        private Id readId(VortexKeys key) {
            return readId(column(key));
        }

        private void writeIds(VortexKeys key, Collection<Id> value) {
            this.entry.column(formatColumnName(key), writeIds(value));
        }

        private Id[] readIds(VortexKeys key) {
            return readIds(column(key));
        }

        private void writeBool(VortexKeys key, boolean value) {
            this.entry.column(formatColumnName(key),
                              new byte[]{(byte) (value ? 1 : 0)});
        }

        private boolean readBool(VortexKeys key) {
            byte[] value = column(key);
            E.checkState(value.length == 1,
                         "The length of column '%s' must be 1, but is '%s'",
                         key, value.length);
            return value[0] != (byte) 0;
        }

        private byte[] writeId(Id id) {
            int size = 1 + id.length();
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeId(id);
            return buffer.bytes();
        }

        private Id readId(byte[] value) {
            BytesBuffer buffer = BytesBuffer.wrap(value);
            return buffer.readId();
        }

        private byte[] writeIds(Collection<Id> ids) {
            E.checkState(ids.size() <= BytesBuffer.UINT16_MAX,
                         "The number of properties of vertex/edge label " +
                         "can't exceed '%s'", BytesBuffer.UINT16_MAX);
            int size = 2;
            for (Id id : ids) {
                size += (1 + id.length());
            }
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeUInt16(ids.size());
            for (Id id : ids) {
                buffer.writeId(id);
            }
            return buffer.bytes();
        }

        private Id[] readIds(byte[] value) {
            BytesBuffer buffer = BytesBuffer.wrap(value);
            int size = buffer.readUInt16();
            Id[] ids = new Id[size];
            for (int i = 0; i < size; i++) {
                Id id = buffer.readId();
                ids[i] = id;
            }
            return ids;
        }

        private byte[] column(VortexKeys key) {
            BackendColumn column = this.entry.column(formatColumnName(key));
            E.checkState(column != null, "Not found key '%s' from entry %s",
                         key, this.entry);
            E.checkNotNull(column.value, "column.value");
            return column.value;
        }

        private byte[] formatColumnName(VortexKeys key) {
            Id id = this.entry.id().origin();
            int size = 1 + id.length() + 1;
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeId(id);
            buffer.write(key.code());
            return buffer.bytes();
        }
    }
}
