package com.vortex.client.util;

import com.vortex.common.util.E;

import java.util.Map;

public final class CommonUtil {

    public static void checkMapClass(Object object, Class<?> kClass,
                                     Class<?> vClass) {
        E.checkArgumentNotNull(object, "The object can't be null");
        E.checkArgument(object instanceof Map,
                        "The object must be instance of Map, but got '%s'(%s)",
                        object, object.getClass());
        E.checkArgumentNotNull(kClass, "The key class can't be null");
        E.checkArgumentNotNull(vClass, "The value class can't be null");
        Map<?, ?> map = (Map<?, ?>) object;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            E.checkState(kClass.isAssignableFrom(key.getClass()),
                         "The map key must be instance of %s, " +
                         "but got '%s'(%s)", kClass, key, key.getClass());
            E.checkState(vClass.isAssignableFrom(value.getClass()),
                         "The map value must be instance of %s, " +
                         "but got '%s'(%s)", vClass, value, value.getClass());
        }
    }
}
