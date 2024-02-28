
package com.vortex.vortexdb.schema;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.schema.builder.SchemaBuilder;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.IdStrategy;
import com.google.common.base.Objects;

import java.util.*;

public class VertexLabel extends SchemaLabel {

    public static final VertexLabel NONE = new VertexLabel(null, NONE_ID, UNDEF);

    // OLAP_VL_ID means all of vertex label ids
    private static final Id OLAP_VL_ID = IdGenerator.of(SchemaLabel.OLAP_VL_ID);
    // OLAP_VL_NAME means all of vertex label names
    private static final String OLAP_VL_NAME = "~olap";
    // OLAP_VL means all of vertex labels
    public static final VertexLabel OLAP_VL = new VertexLabel(null, OLAP_VL_ID,
                                                              OLAP_VL_NAME);

    private IdStrategy idStrategy;
    private List<Id> primaryKeys;

    public VertexLabel(final Vortex graph, Id id, String name) {
        super(graph, id, name);
        this.idStrategy = IdStrategy.DEFAULT;
        this.primaryKeys = new ArrayList<>();
    }

    @Override
    public VortexType type() {
        return VortexType.VERTEX_LABEL;
    }

    public boolean olap() {
        return VertexLabel.OLAP_VL.id().equals(this.id());
    }

    public IdStrategy idStrategy() {
        return this.idStrategy;
    }

    public void idStrategy(IdStrategy idStrategy) {
        this.idStrategy = idStrategy;
    }

    public List<Id> primaryKeys() {
        return Collections.unmodifiableList(this.primaryKeys);
    }

    public void primaryKey(Id id) {
        this.primaryKeys.add(id);
    }

    public void primaryKeys(Id... ids) {
        this.primaryKeys.addAll(Arrays.asList(ids));
    }

    public boolean existsLinkLabel() {
        return this.graph().existsLinkLabel(this.id());
    }

    public boolean hasSameContent(VertexLabel other) {
        return super.hasSameContent(other) &&
               this.idStrategy == other.idStrategy &&
               Objects.equal(this.graph.mapPkId2Name(this.primaryKeys),
                             other.graph.mapPkId2Name(other.primaryKeys));
    }

    public static VertexLabel undefined(Vortex graph) {
        return new VertexLabel(graph, NONE_ID, UNDEF);
    }

    public static VertexLabel undefined(Vortex graph, Id id) {
        return new VertexLabel(graph, id, UNDEF);
    }

    public interface Builder extends SchemaBuilder<VertexLabel> {

        Id rebuildIndex();

        Builder idStrategy(IdStrategy idStrategy);

        Builder useAutomaticId();

        Builder usePrimaryKeyId();

        Builder useCustomizeStringId();

        Builder useCustomizeNumberId();

        Builder useCustomizeUuidId();

        Builder properties(String... properties);

        Builder primaryKeys(String... keys);

        Builder nullableKeys(String... keys);

        Builder ttl(long ttl);

        Builder ttlStartTime(String ttlStartTime);

        Builder enableLabelIndex(boolean enable);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }
}
