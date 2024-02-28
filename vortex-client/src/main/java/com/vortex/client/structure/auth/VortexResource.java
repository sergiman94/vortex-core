package com.vortex.client.structure.auth;

import com.vortex.client.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

public class VortexResource {

    public static final String ANY = "*";

    @JsonProperty("type")
    private VortexResourceType type = VortexResourceType.NONE;

    @JsonProperty("label")
    private String label = ANY;

    @JsonProperty("properties")
    private Map<String, Object> properties; // value can be predicate

    public VortexResource() {
        // pass
    }

    public VortexResource(VortexResourceType type) {
        this(type, ANY);
    }

    public VortexResource(VortexResourceType type, String label) {
        this(type, label, null);
    }

    public VortexResource(VortexResourceType type, String label,
                          Map<String, Object> properties) {
        this.type = type;
        this.label = label;
        this.properties = properties;
    }


    public VortexResourceType resourceType() {
        return this.type;
    }

    public String label() {
        return this.label;
    }

    public Map<String, Object> properties() {
        return this.properties;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof VortexResource)) {
            return false;
        }
        VortexResource other = (VortexResource) object;
        return this.type == other.type &&
               Objects.equals(this.label, other.label) &&
               Objects.equals(this.properties, other.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.type, this.label, this.properties);
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }
}
