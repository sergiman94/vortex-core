
package com.vortex.vortexdb.job.computer;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class WeakConnectedComponentComputer extends AbstractComputer {

    public static final String WCC = "weak_connected_component";

    @Override
    public String name() {
        return WCC;
    }

    @Override
    public String category() {
        return CATEGORY_COMM;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        maxSteps(parameters);
        precision(parameters);
    }

    @Override
    protected Map<String, Object> checkAndCollectParameters(
                                  Map<String, Object> parameters) {
        return ImmutableMap.of(MAX_STEPS, maxSteps(parameters),
                               PRECISION, precision(parameters));
    }
}
