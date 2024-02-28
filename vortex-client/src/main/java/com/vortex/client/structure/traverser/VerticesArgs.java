package com.vortex.client.structure.traverser;

import com.vortex.common.util.E;

import java.util.*;

public class VerticesArgs {

    public Set<Object> ids;
    public String label;
    public Map<String, Object> properties;

    private VerticesArgs() {
        this.ids = new HashSet<>();
        this.label = null;
        this.properties = new HashMap<>();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("VerticesArgs{ids=%s,label=%s,properties=%s}",
                             this.ids, this.label, this.properties);
    }

    public static class Builder {

        private VerticesArgs vertices;

        private Builder() {
            this.vertices = new VerticesArgs();
        }

        public Builder ids(Set<Object> ids) {
            this.vertices.ids.addAll(ids);
            return this;
        }

        public Builder ids(Object... ids) {
            this.vertices.ids.addAll(Arrays.asList(ids));
            return this;
        }

        public Builder label(String label) {
            this.vertices.label = label;
            return this;
        }

        public Builder properties(Map<String, Object> properties) {
            this.vertices.properties = properties;
            return this;
        }

        public Builder property(String key, Object value) {
            this.vertices.properties.put(key, value);
            return this;
        }

        protected VerticesArgs build() {
            E.checkArgument(!((this.vertices.ids == null ||
                               this.vertices.ids.isEmpty()) &&
                              (this.vertices.properties == null ||
                               this.vertices.properties.isEmpty()) &&
                              (this.vertices.label == null ||
                               this.vertices.label.isEmpty())),
                            "No vertices provided");
            return this.vertices;
        }
    }
}
