package com.vortex.client.api.traverser;

import com.vortex.client.api.API;
import com.vortex.client.client.RestClient;
import com.vortex.common.util.E;

public class TraversersAPI extends API {

    private static final String PATH = "graphs/%s/traversers/%s";

    public TraversersAPI(RestClient client, String graph) {
        super(client);
        this.path(PATH, graph, this.type());
    }

    @Override
    protected String type() {
        return "traversers";
    }

    public static void checkPositive(int value, String name) {
        E.checkArgument(value > 0,
                        "%s must be > 0, but got '%s'", name, value);
    }

    public static void checkDegree(long degree) {
        checkLimit(degree, "Degree");
    }

    public static void checkCapacity(long capacity) {
        checkLimit(capacity, "Capacity");
    }

    public static void checkLimit(long limit) {
        checkLimit(limit, "Limit");
    }

    public static void checkAlpha(double alpha) {
        E.checkArgument(alpha > 0 && alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", alpha);
    }

    public static void checkSkipDegree(long skipDegree, long degree,
                                       long capacity) {
        E.checkArgument(skipDegree >= 0L,
                        "The skipped degree must be >= 0, but got '%s'",
                        skipDegree);
        if (capacity != NO_LIMIT) {
            E.checkArgument(degree != NO_LIMIT && degree < capacity,
                            "The max degree must be < capacity");
            E.checkArgument(skipDegree < capacity,
                            "The skipped degree must be < capacity");
        }
        if (skipDegree > 0L) {
            E.checkArgument(degree != NO_LIMIT && skipDegree >= degree,
                            "The skipped degree must be >= max degree, " +
                            "but got skipped degree '%s' and max degree '%s'",
                            skipDegree, degree);
        }
    }
}
