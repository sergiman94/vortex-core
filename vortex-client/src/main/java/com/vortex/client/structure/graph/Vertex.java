package com.vortex.client.structure.graph;

import com.vortex.client.exception.InvalidOperationException;
import com.vortex.client.structure.GraphElement;
import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class Vertex extends GraphElement {

    @JsonProperty("id")
    private Object id;

    @JsonCreator
    public Vertex(@JsonProperty("label") String label) {
        this.label = label;
        this.type = "vertex";
    }

    public Object id() {
        return this.id;
    }

    public void id(Object id) {
        this.id = id;
    }

    public Edge addEdge(String label, Vertex vertex, Object... properties) {
        E.checkNotNull(label, "The edge label can not be null.");
        E.checkNotNull(vertex, "The target vertex can not be null.");
        return this.manager.addEdge(this, label, vertex, properties);
    }

    public Edge addEdge(String label, Vertex vertex,
                        Map<String, Object> properties) {
        E.checkNotNull(label, "The edge label can not be null.");
        E.checkNotNull(vertex, "The target vertex can not be null.");
        return this.manager.addEdge(this, label, vertex, properties);
    }

    @Override
    public Vertex property(String key, Object value) {
        E.checkNotNull(key, "The property name can not be null");
        E.checkNotNull(value, "The property value can not be null");
        if (this.fresh()) {
            return (Vertex) super.property(key, value);
        } else {
            return this.setProperty(key, value);
        }
    }

    @Override
    protected Vertex setProperty(String key, Object value) {
        Vertex vertex = new Vertex(this.label);
        vertex.id(this.id);
        vertex.property(key, value);
        // NOTE: append can also be used to update property
        vertex = this.manager.appendVertexProperty(vertex);

        super.property(key, vertex.property(key));
        return this;
    }

    @Override
    public Vertex removeProperty(String key) {
        E.checkNotNull(key, "The property name can not be null");
        if (!this.properties.containsKey(key)) {
            throw new InvalidOperationException(
                      "The vertex '%s' doesn't have the property '%s'",
                      this.id, key);
        }
        Vertex vertex = new Vertex(this.label);
        vertex.id(this.id);
        Object value = this.properties.get(key);
        vertex.property(key, value);
        this.manager.eliminateVertexProperty(vertex);

        this.properties().remove(key);
        return this;
    }

    @Override
    public String toString() {
        return String.format("{id=%s, label=%s, properties=%s}",
                             this.id, this.label, this.properties);
    }
}
