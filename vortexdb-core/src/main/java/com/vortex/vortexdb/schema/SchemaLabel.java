
package com.vortex.vortexdb.schema;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.Indexfiable;
import com.vortex.vortexdb.type.Propfiable;
import com.vortex.common.util.E;
import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class SchemaLabel extends SchemaElement
                                  implements Indexfiable, Propfiable {

    private final Set<Id> properties;
    private final Set<Id> nullableKeys;
    private final Set<Id> indexLabels;
    private boolean enableLabelIndex;
    private long ttl;
    private Id ttlStartTime;

    public SchemaLabel(final Vortex graph, Id id, String name) {
        super(graph, id, name);
        this.properties = new HashSet<>();
        this.nullableKeys = new HashSet<>();
        this.indexLabels = new HashSet<>();
        this.enableLabelIndex = true;
        this.ttl = 0L;
        this.ttlStartTime = NONE_ID;
    }

    @Override
    public Set<Id> properties() {
        return Collections.unmodifiableSet(this.properties);
    }

    public void properties(Set<Id> properties) {
        this.properties.addAll(properties);
    }

    public SchemaLabel properties(Id... ids) {
        this.properties.addAll(Arrays.asList(ids));
        return this;
    }

    public void property(Id id) {
        this.properties.add(id);
    }

    public Set<Id> nullableKeys() {
        return Collections.unmodifiableSet(this.nullableKeys);
    }

    public void nullableKey(Id id) {
        this.nullableKeys.add(id);
    }

    public void nullableKeys(Id... ids) {
        this.nullableKeys.addAll(Arrays.asList(ids));
    }

    public void nullableKeys(Set<Id> nullableKeys) {
        this.nullableKeys.addAll(nullableKeys);
    }

    @Override
    public Set<Id> indexLabels() {
        return Collections.unmodifiableSet(this.indexLabels);
    }

    public void indexLabel(Id id) {
        this.indexLabels.add(id);
    }

    public void indexLabels(Id... ids) {
        this.indexLabels.addAll(Arrays.asList(ids));
    }

    public boolean existsIndexLabel() {
        return !this.indexLabels().isEmpty();
    }

    public void removeIndexLabel(Id id) {
        this.indexLabels.remove(id);
    }

    public boolean enableLabelIndex() {
        return this.enableLabelIndex;
    }

    public void enableLabelIndex(boolean enable) {
        this.enableLabelIndex = enable;
    }

    public boolean undefined() {
        return this.name() == UNDEF;
    }

    public void ttl(long ttl) {
        assert ttl >= 0L;
        this.ttl = ttl;
    }

    public long ttl() {
        assert this.ttl >= 0L;
        return this.ttl;
    }

    public void ttlStartTime(Id id) {
        this.ttlStartTime = id;
    }

    public Id ttlStartTime() {
        return this.ttlStartTime;
    }

    public String ttlStartTimeName() {
        return NONE_ID.equals(this.ttlStartTime) ? null :
               this.graph.propertyKey(this.ttlStartTime).name();
    }

    public boolean hasSameContent(SchemaLabel other) {
        return super.hasSameContent(other) && this.ttl == other.ttl &&
               this.enableLabelIndex == other.enableLabelIndex &&
               Objects.equal(this.graph.mapPkId2Name(this.properties),
                             other.graph.mapPkId2Name(other.properties)) &&
               Objects.equal(this.graph.mapPkId2Name(this.nullableKeys),
                             other.graph.mapPkId2Name(other.nullableKeys)) &&
               Objects.equal(this.graph.mapIlId2Name(this.indexLabels),
                             other.graph.mapIlId2Name(other.indexLabels)) &&
               Objects.equal(this.ttlStartTimeName(), other.ttlStartTimeName());
    }

    public static Id getLabelId(Vortex graph, VortexType type, Object label) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(type, "type");
        E.checkNotNull(label, "label");
        if (label instanceof Number) {
            return IdGenerator.of(((Number) label).longValue());
        } else if (label instanceof String) {
            if (type.isVertex()) {
                return graph.vertexLabel((String) label).id();
            } else if (type.isEdge()) {
                return graph.edgeLabel((String) label).id();
            } else {
                throw new VortexException(
                          "Not support query from '%s' with label '%s'",
                          type, label);
            }
        } else {
            throw new VortexException(
                      "The label type must be number or string, but got '%s'",
                      label.getClass());
        }
    }
}
