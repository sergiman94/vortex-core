package com.vortex.client.structure;

import com.vortex.client.driver.GraphManager;
import com.vortex.client.structure.constant.GraphAttachable;
import com.vortex.common.util.E;
import com.vortex.common.util.ReflectionUtil;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class GraphElement extends Element implements GraphAttachable {

    // Hold a graphManager object to call graphApi
    protected GraphManager manager;

    @JsonProperty("label")
    protected String label;
    @JsonProperty("type")
    protected String type;
    @JsonProperty("properties")
    protected Map<String, Object> properties;

    public GraphElement() {
        this.properties = new ConcurrentHashMap<>();
    }

    @Override
    public void attachManager(GraphManager manager) {
        this.manager = manager;
    }

    public String label() {
        return this.label;
    }

    @Override
    public String type() {
        return this.type;
    }

    protected boolean fresh() {
        return this.manager == null;
    }

    public Object property(String key) {
        return this.properties.get(key);
    }

    public GraphElement property(String name, Object value) {
        E.checkArgumentNotNull(name, "property name");
        E.checkArgumentNotNull(value, "property value");

        Class<?> clazz = value.getClass();
        E.checkArgument(ReflectionUtil.isSimpleType(clazz) ||
                        clazz.equals(UUID.class) ||
                        clazz.equals(Date.class) ||
                        value instanceof List ||
                        value instanceof Set,
                        "Invalid property value type: '%s'", clazz);

        this.properties.put(name, value);
        return this;
    }

    public Map<String, Object> properties() {
        return this.properties;
    }

    protected abstract GraphElement setProperty(String key, Object value);

    public abstract GraphElement removeProperty(String key);
}
