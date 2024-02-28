
package com.vortex.vortexdb.util;

import com.vortex.common.util.ReflectionUtil;
import com.vortex.vortexdb.VortexException;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public final class CopyUtil {

    @SuppressWarnings("unchecked")
    public static <T> T cloneObject(T o, T clone) throws Exception {
        if (clone == null) {
            clone = (T) o.getClass().newInstance();
        }
        for (Field field : o.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(o);
            if (value == null || Modifier.isFinal(field.getModifiers())) {
                continue;
            }

            Class<?> declareType = field.getType();
            Class<?> valueType = value.getClass();
            if (ReflectionUtil.isSimpleType(declareType) ||
                ReflectionUtil.isSimpleType(valueType)) {
                field.set(clone, value);
            } else if (declareType.isArray() && valueType.isArray() &&
                       valueType.getComponentType().isPrimitive()) {
                field.set(clone, cloneArray(value));
            } else {
                if (value == o) {
                    field.set(clone, clone);
                } else {
                    field.set(clone, cloneObject(value, null));
                }
            }
        }
        return clone;
    }

    private static Object cloneArray(Object value) {
        Class<?> valueType = value.getClass();
        assert valueType.isArray() &&
               valueType.getComponentType().isPrimitive();
        int len = Array.getLength(value);
        Object array = Array.newInstance(valueType.getComponentType(), len);
        System.arraycopy(value, 0, array, 0, len);
        return array;
    }

    public static <T> T copy(T object) {
        return copy(object, null);
    }

    public static <T> T copy(T object, T clone) {
        try {
            return cloneObject(object, clone);
        } catch (Exception e) {
            throw new VortexException("Failed to clone object", e);
        }
    }

    public static <T> T deepCopy(T object) {
        @SuppressWarnings("unchecked")
        Class<T> cls = (Class<T>) object.getClass();
        return JsonUtil.fromJson(JsonUtil.toJson(object), cls);
    }
}
