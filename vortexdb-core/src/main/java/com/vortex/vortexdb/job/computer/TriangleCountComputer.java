
package com.vortex.vortexdb.job.computer;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TriangleCountComputer extends AbstractComputer {

    public static final String TRIANGLE_COUNT = "triangle_count";

    @Override
    public String name() {
        return TRIANGLE_COUNT;
    }

    @Override
    public String category() {
        return CATEGORY_COMM;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        direction(parameters);
        degree(parameters);
    }

    @Override
    protected Map<String, Object> checkAndCollectParameters(
                                  Map<String, Object> parameters) {
        return ImmutableMap.of(DIRECTION, direction(parameters),
                               DEGREE, degree(parameters));
    }
}
