
package com.vortex.vortexdb.job.computer;

import com.vortex.common.util.E;
import com.vortex.vortexdb.util.ParameterUtil;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class LpaComputer extends AbstractComputer {

    public static final String LPA = "lpa";

    public static final String PROPERTY = "property";
    public static final String DEFAULT_PROPERTY = "id";

    @Override
    public String name() {
        return LPA;
    }

    @Override
    public String category() {
        return CATEGORY_COMM;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        times(parameters);
        property(parameters);
        precision(parameters);
        direction(parameters);
        degree(parameters);
    }

    @Override
    protected Map<String, Object> checkAndCollectParameters(
                                  Map<String, Object> parameters) {
        return ImmutableMap.of(TIMES, times(parameters),
                               PROPERTY, property(parameters),
                               PRECISION, precision(parameters),
                               DIRECTION, direction(parameters),
                               DEGREE, degree(parameters));
    }

    private static String property(Map<String, Object> parameters) {
        if (!parameters.containsKey(PROPERTY)) {
            return DEFAULT_PROPERTY;
        }
        String property = ParameterUtil.parameterString(parameters, PROPERTY);
        E.checkArgument(property != null && !property.isEmpty(),
                        "The value of %s can not be null or empty", PROPERTY);
        return property;
    }
}
