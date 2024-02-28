
package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.auth.SchemaDefine.AuthElement;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.vortexdb.traversal.optimize.TraversalUtil;
import com.vortex.vortexdb.type.Namifiable;
import com.vortex.vortexdb.type.Typifiable;
import com.vortex.vortexdb.util.JsonUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class VortexResource {

    public static final String ANY = "*";

    public static final VortexResource ALL = new VortexResource(ResourceType.ALL,
                                                            ANY, null);
    public static final List<VortexResource> ALL_RES = ImmutableList.of(ALL);

    private static final Set<ResourceType> CHECK_NAME_RESS = ImmutableSet.of(
                                                             ResourceType.META);

    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(VortexResource.class, new VortexResourceSer());
        module.addDeserializer(VortexResource.class, new VortexResourceDeser());

        JsonUtil.registerModule(module);
    }

    @JsonProperty("type")
    private ResourceType type = ResourceType.NONE;

    @JsonProperty("label")
    private String label = ANY;

    @JsonProperty("properties")
    private Map<String, Object> properties; // value can be predicate

    public VortexResource() {
        // pass
    }

    public VortexResource(ResourceType type, String label,
                          Map<String, Object> properties) {
        this.type = type;
        this.label = label;
        this.properties = properties;
        this.checkFormat();
    }

    public void checkFormat() {
        if (this.properties == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
            String propName = entry.getKey();
            Object propValue = entry.getValue();
            if (propName.equals(ANY) && propValue.equals(ANY)) {
                continue;
            }
            if (propValue instanceof String &&
                ((String) propValue).startsWith(TraversalUtil.P_CALL)) {
                TraversalUtil.parsePredicate((String) propValue);
            }
        }
    }

    public boolean filter(ResourceObject<?> resourceObject) {
        if (this.type == null) {
            return false;
        }

        if (!this.type.match(resourceObject.type())) {
            return false;
        }

        if (resourceObject.operated() != NameObject.ANY) {
            ResourceType resType = resourceObject.type();
            if (resType.isGraph()) {
                return this.filter((VortexElement) resourceObject.operated());
            }
            if (resType.isAuth()) {
                return this.filter((AuthElement) resourceObject.operated());
            }
            if (resType.isSchema() || CHECK_NAME_RESS.contains(resType)) {
                return this.filter((Namifiable) resourceObject.operated());
            }
        }

        /*
         * Allow any others resource if the type is matched:
         * VAR, GREMLIN, GREMLIN_JOB, TASK
         */
        return true;
    }

    private boolean filter(AuthElement element) {
        assert this.type.match(element.type());
        if (element instanceof Namifiable) {
            if (!this.filter((Namifiable) element)) {
                return false;
            }
        }
        return true;
    }

    private boolean filter(Namifiable element) {
        assert !(element instanceof Typifiable) || this.type.match(
               ResourceType.from(((Typifiable) element).type()));

        if (!this.matchLabel(element.name())) {
            return false;
        }
        return true;
    }

    private boolean filter(VortexElement element) {
        assert this.type.match(ResourceType.from(element.type()));

        if (!this.matchLabel(element.label())) {
            return false;
        }

        if (this.properties == null) {
            return true;
        }
        for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
            String propName = entry.getKey();
            Object expected = entry.getValue();
            if (propName.equals(ANY) && expected.equals(ANY)) {
                return true;
            }
            Property<Object> prop = element.property(propName);
            if (!prop.isPresent()) {
                return false;
            }
            try {
                if (!TraversalUtil.testProperty(prop, expected)) {
                    return false;
                }
            } catch (IllegalArgumentException e) {
                throw new VortexException("Invalid resource '%s' for '%s': %s",
                                        expected, propName, e.getMessage());
            }
        }
        return true;
    }

    private boolean matchLabel(String other) {
        // Label value may be vertex/edge label or schema name
        if (this.label == null || other == null) {
            return false;
        }
        // It's ok if wildcard match or regular match
        if (!this.label.equals(ANY) && !other.matches(this.label)) {
            return false;
        }
        return true;
    }

    private boolean matchProperties(Map<String, Object> other) {
        if (this.properties == null) {
            // Any property is OK
            return true;
        }
        if (other == null) {
            return false;
        }
        for (Map.Entry<String, Object> p : other.entrySet()) {
            Object value = this.properties.get(p.getKey());
            if (!Objects.equals(value, p.getValue())) {
                return false;
            }
        }
        return true;
    }

    protected boolean contains(VortexResource other) {
        if (this.equals(other)) {
            return true;
        }
        if (this.type == null) {
            return false;
        }
        if (!this.type.match(other.type)) {
            return false;
        }
        if (!this.matchLabel(other.label)) {
            return false;
        }
        if (!this.matchProperties(other.properties)) {
            return false;
        }
        return true;
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

    public static boolean allowed(ResourceObject<?> resourceObject) {
        // Allowed to access system(hidden) schema by anyone
        if (resourceObject.type().isSchema()) {
            Namifiable schema = (Namifiable) resourceObject.operated();
            if (Hidden.isHidden(schema.name())) {
                return true;
            }
        }

        return false;
    }

    public static VortexResource parseResource(String resource) {
        return JsonUtil.fromJson(resource, VortexResource.class);
    }

    public static List<VortexResource> parseResources(String resources) {
        TypeReference<?> type = new TypeReference<List<VortexResource>>() {};
        return JsonUtil.fromJson(resources, type);
    }

    public static class NameObject implements Namifiable {

        public static final NameObject ANY = new NameObject("*");

        private final String name;

        public static NameObject of(String name) {
            return new NameObject(name);
        }

        private NameObject(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }

    private static class VortexResourceSer extends StdSerializer<VortexResource> {

        private static final long serialVersionUID = -138482122210181714L;

        public VortexResourceSer() {
            super(VortexResource.class);
        }

        @Override
        public void serialize(VortexResource res, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeStartObject();

            generator.writeObjectField("type", res.type);
            generator.writeObjectField("label", res.label);
            generator.writeObjectField("properties", res.properties);

            generator.writeEndObject();
        }
    }

    private static class VortexResourceDeser extends StdDeserializer<VortexResource> {

        private static final long serialVersionUID = -2499038590503066483L;

        public VortexResourceDeser() {
            super(VortexResource.class);
        }

        @Override
        public VortexResource deserialize(JsonParser parser,
                                          DeserializationContext ctxt)
                                        throws IOException {
            VortexResource res = new VortexResource();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String key = parser.getCurrentName();
                if (key.equals("type")) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        res.type = ctxt.readValue(parser, ResourceType.class);
                    } else {
                        res.type = null;
                    }
                } else if (key.equals("label")) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        res.label = parser.getValueAsString();
                    } else {
                        res.label = null;
                    }
                } else if (key.equals("properties")) {
                    if (parser.nextToken() != JsonToken.VALUE_NULL) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> prop = ctxt.readValue(parser,
                                                                  Map.class);
                        res.properties = prop;
                    } else {
                        res.properties = null;
                    }
                }
            }
            res.checkFormat();
            return res;
        }
    }
}
