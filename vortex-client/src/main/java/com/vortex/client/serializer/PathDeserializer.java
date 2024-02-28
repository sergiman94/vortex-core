package com.vortex.client.serializer;

import com.vortex.client.exception.InvalidResponseException;
import com.vortex.client.structure.graph.Edge;
import com.vortex.client.structure.graph.Path;
import com.vortex.client.structure.graph.Vertex;
import com.vortex.client.util.JsonUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PathDeserializer extends JsonDeserializer<Path> {

    @Override
    public Path deserialize(JsonParser parser, DeserializationContext ctxt)
                            throws IOException {

        JsonNode node = parser.getCodec().readTree(parser);
        Path path = new Path();

        // Parse node 'labels'
        JsonNode labelsNode = node.get("labels");
        if (labelsNode != null) {
            if (labelsNode.getNodeType() != JsonNodeType.ARRAY) {
                throw InvalidResponseException.expectField("labels", node);
            }
            Object labels = JsonUtil.convertValue(labelsNode, Object.class);
            ((List<?>) labels).forEach(path::labels);
        }

        // Parse node 'objects'
        JsonNode objectsNode = node.get("objects");
        if (objectsNode == null ||
            objectsNode.getNodeType() != JsonNodeType.ARRAY) {
            throw InvalidResponseException.expectField("objects", node);
        }

        Iterator<JsonNode> objects = objectsNode.elements();
        while (objects.hasNext()) {
            JsonNode objectNode = objects.next();
            JsonNode typeNode = objectNode.get("type");
            Object object;
            if (typeNode != null) {
                object = parseTypedNode(objectNode, typeNode);
            } else {
                object = JsonUtil.convertValue(objectNode, Object.class);
            }
            path.objects(object);
        }

        // Parse node 'crosspoint'
        JsonNode crosspointNode = node.get("crosspoint");
        if (crosspointNode != null) {
            Object object = JsonUtil.convertValue(crosspointNode, Object.class);
            path.crosspoint(object);
        }
        return path;
    }

    private Object parseTypedNode(JsonNode objectNode, JsonNode typeNode) {
        String type = typeNode.asText();
        if ("vertex".equals(type)) {
            return JsonUtil.convertValue(objectNode, Vertex.class);
        } else if ("edge".equals(type)) {
            return JsonUtil.convertValue(objectNode, Edge.class);
        } else {
            throw InvalidResponseException.expectField("vertex/edge", type);
        }
    }
}
