
package com.vortex.vortexdb.util;

import com.vortex.common.util.E;

import java.util.Map;

public class ParameterUtil {

    public static Object parameter(Map<String, Object> parameters, String key) {
        Object value = parameters.get(key);
        E.checkArgument(value != null,
                        "Expect '%s' in parameters: %s",
                        key, parameters);
        return value;
    }

    public static String parameterString(Map<String, Object> parameters,
                                         String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof String,
                        "Expect string value for parameter '%s': '%s'",
                        key, value);
        return (String) value;
    }

    public static int parameterInt(Map<String, Object> parameters,
                                   String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect int value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).intValue();
    }

    public static long parameterLong(Map<String, Object> parameters,
                                     String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect long value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).longValue();
    }

    public static double parameterDouble(Map<String, Object> parameters,
                                         String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect double value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).doubleValue();
    }

    public static boolean parameterBoolean(Map<String, Object> parameters,
                                           String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Boolean,
                        "Expect boolean value for parameter '%s': '%s'",
                        key, value);
        return ((Boolean) value);
    }
}
