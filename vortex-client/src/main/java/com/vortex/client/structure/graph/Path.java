package com.vortex.client.structure.graph;

import com.vortex.client.driver.GraphManager;
import com.vortex.client.structure.constant.GraphAttachable;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class Path implements GraphAttachable {

    @JsonProperty
    private List<Object> labels;
    @JsonProperty
    private List<Object> objects;
    @JsonProperty
    private Object crosspoint;

    public Path() {
        this(ImmutableList.of());
    }

    public Path(List<Object> objects) {
        this(null, objects);
    }

    public Path(Object crosspoint, List<Object> objects) {
        this.crosspoint = crosspoint;
        this.labels = new CopyOnWriteArrayList<>();
        this.objects = new CopyOnWriteArrayList<>(objects);
    }

    public List<Object> labels() {
        return Collections.unmodifiableList(this.labels);
    }

    public void labels(Object... labels) {
        this.labels.addAll(Arrays.asList(labels));
    }

    public List<Object> objects() {
        return Collections.unmodifiableList(this.objects);
    }

    public void objects(Object... objects) {
        this.objects.addAll(Arrays.asList(objects));
    }

    public Object crosspoint() {
        return this.crosspoint;
    }

    public void crosspoint(Object crosspoint) {
        this.crosspoint = crosspoint;
    }

    public int size() {
        return this.objects.size();
    }

    @Override
    public void attachManager(GraphManager manager) {
        for (Object object : this.objects) {
            if (object instanceof GraphAttachable) {
                ((GraphAttachable) object).attachManager(manager);
            }
        }
        if (this.crosspoint instanceof GraphAttachable) {
            ((GraphAttachable) this.crosspoint).attachManager(manager);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof Path)) {
            return false;
        }
        Path other = (Path) object;
        return Objects.equals(this.labels, other.labels) &&
               Objects.equals(this.objects, other.objects) &&
               Objects.equals(this.crosspoint, other.crosspoint);
    }

    @Override
    public String toString() {
        return String.format("{labels=%s, objects=%s, crosspoint=%s}",
                             this.labels, this.objects, this.crosspoint);
    }
}
