package com.vortex.client.structure.gremlin;

import com.vortex.client.driver.GraphManager;
import com.vortex.common.rest.SerializeException;
import com.vortex.client.serializer.PathDeserializer;
import com.vortex.client.structure.constant.GraphAttachable;
import com.vortex.client.structure.graph.Edge;
import com.vortex.client.structure.graph.Path;
import com.vortex.client.structure.graph.Vertex;
import com.vortex.common.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.util.*;

public class ResultSet {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private GraphManager graphManager = null;

    @JsonProperty
    private List<Object> data;
    @JsonProperty
    private Map<String, ?> meta;

    static {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Path.class, new PathDeserializer());
        MAPPER.registerModule(module);
    }

    public void graphManager(GraphManager graphManager) {
        this.graphManager = graphManager;
    }

    public List<Object> data() {
        return this.data;
    }

    public int size() {
        return this.data.size();
    }

    public Result get(int index) {
        if (index >= this.data.size()) {
            return null;
        }

        Object object = this.data().get(index);
        if (object == null) {
            return null;
        }

        Class<?> clazz = this.parseResultClass(object);
        // Primitive type
        if (clazz.equals(object.getClass())) {
            return new Result(object);
        }

        try {
            String rawValue = MAPPER.writeValueAsString(object);
            object = MAPPER.readValue(rawValue, clazz);
            if (object instanceof GraphAttachable) {
                ((GraphAttachable) object).attachManager(graphManager);
            }
            return new Result(object);
        } catch (Exception e) {
            throw new SerializeException(
                      "Failed to deserialize: %s", e, object);
        }
    }

    /**
     * TODO: Still need to constantly add and optimize
     */
    private Class<?> parseResultClass(Object object) {
        if (object.getClass().equals(LinkedHashMap.class)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            String type = (String) map.get("type");
            if (type != null) {
                if ("vertex".equals(type)) {
                    return Vertex.class;
                } else if ("edge".equals(type)) {
                    return Edge.class;
                }
            } else {
                if (map.get("labels") != null) {
                    return Path.class;
                }
            }
        }

        return object.getClass();
    }

    public Iterator<Result> iterator() {
        E.checkState(this.data != null, "Invalid response from server");
        E.checkState(this.graphManager != null, "Must hold a graph manager");

        return new Iterator<Result>() {

            private int index = 0;

            @Override
            public boolean hasNext() {
                return this.index < ResultSet.this.data.size();
            }

            @Override
            public Result next() {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
                return get(this.index++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
