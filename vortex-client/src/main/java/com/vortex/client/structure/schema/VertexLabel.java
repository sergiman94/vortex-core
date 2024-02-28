package com.vortex.client.structure.schema;

import com.vortex.client.driver.SchemaManager;
import com.vortex.client.structure.constant.VortexType;
import com.vortex.client.structure.constant.IdStrategy;
import com.vortex.common.util.CollectionUtil;
import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class VertexLabel extends SchemaLabel {

    @JsonProperty("id_strategy")
    private IdStrategy idStrategy;
    @JsonProperty("primary_keys")
    private List<String> primaryKeys;
    @JsonProperty("ttl")
    private long ttl;
    @JsonProperty("ttl_start_time")
    private String ttlStartTime;

    @JsonCreator
    public VertexLabel(@JsonProperty("name") String name) {
        super(name);
        this.idStrategy = IdStrategy.DEFAULT;
        this.primaryKeys = new CopyOnWriteArrayList<>();
        this.ttl = 0L;
        this.ttlStartTime = null;
    }

    @Override
    public String type() {
        return VortexType.VERTEX_LABEL.string();
    }

    public IdStrategy idStrategy() {
        return this.idStrategy;
    }

    public List<String> primaryKeys() {
        return this.primaryKeys;
    }

    public long ttl() {
        return this.ttl;
    }

    public String ttlStartTime() {
        return this.ttlStartTime;
    }

    @Override
    public String toString() {
        return String.format("{name=%s, idStrategy=%s, primaryKeys=%s, " +
                             "indexLabels=%s, nullableKeys=%s, properties=%s," +
                             " ttl=%s, ttlStartTime=%s, status=%s}",
                             this.name, this.idStrategy, this.primaryKeys,
                             this.indexLabels, this.nullableKeys,
                             this.properties, this.ttl, this.ttlStartTime,
                             this.status);
    }

    public VertexLabelV53 switchV53() {
        return new VertexLabelV53(this);
    }

    public interface Builder extends SchemaBuilder<VertexLabel> {

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

        Builder userdata(String key, Object val);

        Builder ifNotExist();
    }

    public static class BuilderImpl implements Builder {

        private VertexLabel vertexLabel;
        private SchemaManager manager;

        public BuilderImpl(String name, SchemaManager manager) {
            this.vertexLabel = new VertexLabel(name);
            this.manager = manager;
        }

        @Override
        public VertexLabel build() {
            return this.vertexLabel;
        }

        @Override
        public VertexLabel create() {
            return this.manager.addVertexLabel(this.vertexLabel);
        }

        @Override
        public VertexLabel append() {
            return this.manager.appendVertexLabel(this.vertexLabel);
        }

        @Override
        public VertexLabel eliminate() {
            return this.manager.eliminateVertexLabel(this.vertexLabel);
        }

        @Override
        public void remove() {
            this.manager.removeVertexLabel(this.vertexLabel.name);
        }

        @Override
        public Builder idStrategy(IdStrategy idStrategy) {
            this.checkIdStrategy();
            this.vertexLabel.idStrategy = idStrategy;
            return this;
        }

        @Override
        public Builder useAutomaticId() {
            this.checkIdStrategy();
            this.vertexLabel.idStrategy = IdStrategy.AUTOMATIC;
            return this;
        }

        @Override
        public Builder usePrimaryKeyId() {
            this.checkIdStrategy();
            this.vertexLabel.idStrategy = IdStrategy.PRIMARY_KEY;
            return this;
        }

        @Override
        public Builder useCustomizeStringId() {
            this.checkIdStrategy();
            this.vertexLabel.idStrategy = IdStrategy.CUSTOMIZE_STRING;
            return this;
        }

        @Override
        public Builder useCustomizeNumberId() {
            this.checkIdStrategy();
            this.vertexLabel.idStrategy = IdStrategy.CUSTOMIZE_NUMBER;
            return this;
        }

        @Override
        public Builder useCustomizeUuidId() {
            this.checkIdStrategy();
            this.vertexLabel.idStrategy = IdStrategy.CUSTOMIZE_UUID;
            return this;
        }

        @Override
        public Builder properties(String... properties) {
            this.vertexLabel.properties.addAll(Arrays.asList(properties));
            return this;
        }

        @Override
        public Builder primaryKeys(String... keys) {
            E.checkArgument(this.vertexLabel.primaryKeys.isEmpty(),
                            "Not allowed to assign primary keys multi times");
            List<String> primaryKeys = Arrays.asList(keys);
            E.checkArgument(CollectionUtil.allUnique(primaryKeys),
                            "Invalid primary keys %s, which contains some " +
                            "duplicate properties", primaryKeys);
            this.vertexLabel.primaryKeys.addAll(primaryKeys);
            return this;
        }

        @Override
        public Builder nullableKeys(String... keys) {
            this.vertexLabel.nullableKeys.addAll(Arrays.asList(keys));
            return this;
        }

        @Override
        public Builder ttl(long ttl) {
            E.checkArgument(ttl >= 0L, "The ttl must >= 0, but got: %s", ttl);
            this.vertexLabel.ttl = ttl;
            return this;
        }

        @Override
        public Builder ttlStartTime(String ttlStartTime) {
            this.vertexLabel.ttlStartTime = ttlStartTime;
            return this;
        }

        @Override
        public Builder enableLabelIndex(boolean enable) {
            this.vertexLabel.enableLabelIndex = enable;
            return this;
        }

        @Override
        public Builder userdata(String key, Object val) {
            E.checkArgumentNotNull(key, "The user data key can't be null");
            E.checkArgumentNotNull(val, "The user data value can't be null");
            this.vertexLabel.userdata.put(key, val);
            return this;
        }

        @Override
        public Builder ifNotExist() {
            this.vertexLabel.checkExist = false;
            return this;
        }

        private void checkIdStrategy() {
            E.checkArgument(this.vertexLabel.idStrategy == IdStrategy.DEFAULT,
                            "Not allowed to change id strategy for " +
                            "vertex label '%s'", this.vertexLabel.name);
        }
    }

    public static class VertexLabelV53 extends SchemaLabel {

        @JsonProperty("id_strategy")
        private IdStrategy idStrategy;
        @JsonProperty("primary_keys")
        private List<String> primaryKeys;

        @JsonCreator
        public VertexLabelV53(@JsonProperty("name") String name) {
            super(name);
            this.idStrategy = IdStrategy.DEFAULT;
            this.primaryKeys = new CopyOnWriteArrayList<>();
        }

        private VertexLabelV53(VertexLabel vertexLabel) {
            super(vertexLabel.name);
            this.idStrategy = vertexLabel.idStrategy;
            this.primaryKeys = vertexLabel.primaryKeys;
            this.id = vertexLabel.id();
            this.properties = vertexLabel.properties();
            this.userdata = vertexLabel.userdata();
            this.checkExist = vertexLabel.checkExist();
            this.nullableKeys = vertexLabel.nullableKeys;
            this.enableLabelIndex = vertexLabel.enableLabelIndex;
        }

        public IdStrategy idStrategy() {
            return this.idStrategy;
        }

        public List<String> primaryKeys() {
            return this.primaryKeys;
        }

        @Override
        public String toString() {
            return String.format("{name=%s, idStrategy=%s, primaryKeys=%s, " +
                                 "nullableKeys=%s, properties=%s}",
                                 this.name, this.idStrategy, this.primaryKeys,
                                 this.nullableKeys, this.properties);
        }

        @Override
        public String type() {
            return VortexType.VERTEX_LABEL.string();
        }
    }
}
