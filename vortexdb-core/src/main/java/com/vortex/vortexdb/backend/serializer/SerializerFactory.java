
package com.vortex.vortexdb.backend.serializer;

import com.vortex.vortexdb.backend.BackendException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SerializerFactory {

    private static Map<String, Class<? extends AbstractSerializer>> serializers;

    static {
        serializers = new ConcurrentHashMap<>();
    }

    public static AbstractSerializer serializer(String name) {
        name = name.toLowerCase();
        if ("binary".equals(name)) {
            return new BinarySerializer();
        } else if ("binaryscatter".equals(name)) {
            return new BinaryScatterSerializer();
        } else if ("text".equals(name)) {
            return new TextSerializer();
        }

        Class<? extends AbstractSerializer> clazz = serializers.get(name);
        if (clazz == null) {
            throw new BackendException("Not exists serializer: '%s'", name);
        }

        assert AbstractSerializer.class.isAssignableFrom(clazz);
        try {
            return clazz.getConstructor().newInstance();
        } catch (Exception e) {
            throw new BackendException(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void register(String name, String classPath) {
        ClassLoader classLoader = SerializerFactory.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new BackendException("Invalid class: '%s'", e, classPath);
        }

        // Check subclass
        if (!AbstractSerializer.class.isAssignableFrom(clazz)) {
            throw new BackendException("Class is not a subclass of class " +
                                       "AbstractSerializer: '%s'", classPath);
        }

        // Check exists
        if (serializers.containsKey(name)) {
            throw new BackendException("Exists serializer: %s(Class '%s')",
                                       name, serializers.get(name).getName());
        }

        // Register class
        serializers.put(name, (Class) clazz);
    }
}
