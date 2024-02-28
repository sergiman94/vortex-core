
package com.vortex.vortexdb.structure;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.Id.IdType;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.id.SplicingIdGenerator;
import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.DataType;
import com.vortex.common.util.E;
import com.vortex.common.util.HashUtil;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.common.util.NumericUtil;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class VortexIndex implements GraphType, Cloneable {

    private final Vortex graph;
    private Object fieldValues;
    private IndexLabel indexLabel;
    private Set<IdWithExpiredTime> elementIds;

    public VortexIndex(Vortex graph, IndexLabel indexLabel) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(indexLabel, "label");
        E.checkNotNull(indexLabel.id(), "label id");
        this.graph = graph;
        this.indexLabel = indexLabel;
        this.elementIds = new LinkedHashSet<>();
        this.fieldValues = null;
    }

    @Override
    public String name() {
        return this.indexLabel.name();
    }

    @Override
    public VortexType type() {
        if (this.indexLabel == IndexLabel.label(VortexType.VERTEX)) {
            return VortexType.VERTEX_LABEL_INDEX;
        } else if (this.indexLabel == IndexLabel.label(VortexType.EDGE)) {
            return VortexType.EDGE_LABEL_INDEX;
        }
        return this.indexLabel.indexType().type();
    }

    public Vortex graph() {
        return this.graph;
    }

    public Id id() {
        return formatIndexId(type(), this.indexLabelId(), this.fieldValues());
    }

    public Id hashId() {
        return formatIndexHashId(type(), this.indexLabelId(), this.fieldValues());
    }

    public Object fieldValues() {
        return this.fieldValues;
    }

    public void fieldValues(Object fieldValues) {
        this.fieldValues = fieldValues;
    }

    public Id indexLabelId() {
        return this.indexLabel.id();
    }

    public IndexLabel indexLabel() {
        return this.indexLabel;
    }

    public IdWithExpiredTime elementIdWithExpiredTime() {
        E.checkState(this.elementIds.size() == 1,
                     "Expect one element id, actual %s",
                     this.elementIds.size());
        return this.elementIds.iterator().next();
    }

    public Id elementId() {
        return this.elementIdWithExpiredTime().id();
    }

    public Set<Id> elementIds() {
        Set<Id> ids = InsertionOrderUtil.newSet(this.elementIds.size());
        for (IdWithExpiredTime idWithExpiredTime : this.elementIds) {
            ids.add(idWithExpiredTime.id());
        }
        return Collections.unmodifiableSet(ids);
    }

    public Set<IdWithExpiredTime> expiredElementIds() {
        long now = this.graph.now();
        Set<IdWithExpiredTime> expired = InsertionOrderUtil.newSet();
        for (IdWithExpiredTime id : this.elementIds) {
            if (0L < id.expiredTime && id.expiredTime < now) {
                expired.add(id);
            }
        }
        this.elementIds.removeAll(expired);
        return expired;
    }

    public void elementIds(Id elementId) {
        this.elementIds(elementId, 0L);
    }

    public void elementIds(Id elementId, long expiredTime) {
        this.elementIds.add(new IdWithExpiredTime(elementId, expiredTime));
    }

    public void resetElementIds() {
        this.elementIds = new LinkedHashSet<>();
    }

    public long expiredTime() {
        return this.elementIdWithExpiredTime().expiredTime();
    }

    public boolean hasTtl() {
        if (this.indexLabel.system()) {
            return false;
        }
        return this.indexLabel.baseElement().ttl() > 0L;
    }

    public long ttl() {
        return this.expiredTime() - this.graph.now();
    }

    @Override
    public VortexIndex clone() {
        try {
            return (VortexIndex) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new VortexException("Failed to clone VortexIndex", e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VortexIndex)) {
            return false;
        }

        VortexIndex other = (VortexIndex) obj;
        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        return this.id().hashCode();
    }

    @Override
    public String toString() {
        return String.format("{label=%s<%s>, fieldValues=%s, elementIds=%s}",
                             this.indexLabel.name(),
                             this.indexLabel.indexType().string(),
                             this.fieldValues, this.elementIds);
    }

    public static Id formatIndexHashId(VortexType type, Id indexLabel,
                                       Object fieldValues) {
        E.checkState(!type.isRangeIndex(),
                     "RangeIndex can't return a hash id");
        String value = fieldValues == null ? "" : fieldValues.toString();
        return formatIndexId(type, indexLabel, HashUtil.hash(value));
    }

    public static Id formatIndexId(VortexType type, Id indexLabelId,
                                   Object fieldValues) {
        if (type.isStringIndex()) {
            String value = "";
            if (fieldValues instanceof Id) {
                value = IdGenerator.asStoredString((Id) fieldValues);
            } else if (fieldValues != null) {
                value = fieldValues.toString();
            }
            /*
             * Modify order between index label and field-values to put the
             * index label in front(vortex-1317)
             */
            String strIndexLabelId = IdGenerator.asStoredString(indexLabelId);
            return SplicingIdGenerator.splicing(strIndexLabelId, value);
        } else {
            assert type.isRangeIndex();
            int length = type.isRange4Index() ? 4 : 8;
            BytesBuffer buffer = BytesBuffer.allocate(4 + length);
            buffer.writeInt(SchemaElement.schemaId(indexLabelId));
            if (fieldValues != null) {
                E.checkState(fieldValues instanceof Number,
                             "Field value of range index must be number:" +
                             " %s", fieldValues.getClass().getSimpleName());
                byte[] bytes = number2bytes((Number) fieldValues);
                buffer.write(bytes);
            }
            return buffer.asId();
        }
    }

    public static VortexIndex parseIndexId(Vortex graph, VortexType type,
                                           byte[] id) {
        Object values;
        IndexLabel indexLabel;
        if (type.isStringIndex()) {
            Id idObject = IdGenerator.of(id, IdType.STRING);
            String[] parts = SplicingIdGenerator.parse(idObject);
            E.checkState(parts.length == 2, "Invalid secondary index id");
            Id label = IdGenerator.ofStoredString(parts[0], IdType.LONG);
            indexLabel = IndexLabel.label(graph, label);
            values = parts[1];
        } else {
            assert type.isRange4Index() || type.isRange8Index();
            final int labelLength = 4;
            E.checkState(id.length > labelLength, "Invalid range index id");
            BytesBuffer buffer = BytesBuffer.wrap(id);
            Id label = IdGenerator.of(buffer.readInt());
            indexLabel = IndexLabel.label(graph, label);
            List<Id> fields = indexLabel.indexFields();
            E.checkState(fields.size() == 1, "Invalid range index fields");
            DataType dataType = graph.propertyKey(fields.get(0)).dataType();
            E.checkState(dataType.isNumber() || dataType.isDate(),
                         "Invalid range index field type");
            Class<?> clazz = dataType.isNumber() ?
                             dataType.clazz() : DataType.LONG.clazz();
            values = bytes2number(buffer.read(id.length - labelLength), clazz);
        }
        VortexIndex index = new VortexIndex(graph, indexLabel);
        index.fieldValues(values);
        return index;
    }

    public static byte[] number2bytes(Number number) {
        if (number instanceof Byte) {
            // Handle byte as integer to store as 4 bytes in RANGE4_INDEX
            number = number.intValue();
        }
        return NumericUtil.numberToSortableBytes(number);
    }

    public static Number bytes2number(byte[] bytes, Class<?> clazz) {
        return NumericUtil.sortableBytesToNumber(bytes, clazz);
    }

    public static class IdWithExpiredTime {

        private Id id;
        private long expiredTime;

        public IdWithExpiredTime(Id id, long expiredTime) {
            this.id = id;
            this.expiredTime = expiredTime;
        }

        public Id id() {
            return this.id;
        }

        public long expiredTime() {
            return this.expiredTime;
        }

        @Override
        public String toString() {
            return String.format("%s(%s)", this.id, this.expiredTime);
        }
    }
}
