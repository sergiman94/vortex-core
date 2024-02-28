package com.vortex.client.util;

import com.vortex.common.rest.SerializeException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public final class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void registerModule(Module module) {
        MAPPER.registerModule(module);
    }

    public static String toJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new SerializeException("Failed to serialize object '%s'",
                                         e, object);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new SerializeException("Failed to deserialize json '%s'",
                                         e, json);
        }
    }

    public static <T> T convertValue(JsonNode node, Class<T> clazz) {
        try {
            return MAPPER.convertValue(node, clazz);
        } catch (IllegalArgumentException e) {
            throw new SerializeException("Failed to deserialize json node '%s'",
                                         e, node);
        }
    }
}
