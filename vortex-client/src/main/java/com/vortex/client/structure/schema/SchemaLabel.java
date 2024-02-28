package com.vortex.client.structure.schema;

import com.vortex.client.structure.SchemaElement;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class SchemaLabel extends SchemaElement {

    @JsonProperty("nullable_keys")
    protected Set<String> nullableKeys;
    @JsonProperty("enable_label_index")
    protected Boolean enableLabelIndex;
    @JsonProperty("index_labels")
    protected List<String> indexLabels;

    public SchemaLabel(String name) {
        super(name);
        this.nullableKeys = new ConcurrentSkipListSet<>();
        this.indexLabels = null;
        this.enableLabelIndex = null;
    }

    public Set<String> nullableKeys() {
        return this.nullableKeys;
    }

    public List<String> indexLabels() {
        if (this.indexLabels == null) {
            return ImmutableList.of();
        }
        return Collections.unmodifiableList(this.indexLabels);
    }

    public boolean enableLabelIndex() {
        return this.enableLabelIndex;
    }
}
