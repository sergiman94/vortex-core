
package com.vortex.vortexdb.job.computer;

import com.vortex.common.util.E;
import com.vortex.vortexdb.util.ParameterUtil;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class PageRankComputer extends AbstractComputer {

    public static final String PAGE_RANK = "page_rank";

    public static final String ALPHA = "alpha";
    public static final double DEFAULT_ALPHA = 0.15D;

    @Override
    public String name() {
        return PAGE_RANK;
    }

    @Override
    public String category() {
        return CATEGORY_RANK;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        maxSteps(parameters);
        alpha(parameters);
        precision(parameters);
    }

    @Override
    protected Map<String, Object> checkAndCollectParameters(
                                  Map<String, Object> parameters) {
        return ImmutableMap.of(MAX_STEPS, maxSteps(parameters),
                               ALPHA, alpha(parameters),
                               PRECISION, precision(parameters));
    }

    private static double alpha(Map<String, Object> parameters) {
        if (!parameters.containsKey(ALPHA)) {
            return DEFAULT_ALPHA;
        }
        double alpha = ParameterUtil.parameterDouble(parameters, ALPHA);
        E.checkArgument(alpha > 0 && alpha < 1,
                        "The value of %s must be (0, 1), but got %s",
                        ALPHA, alpha);
        return alpha;
    }
}
