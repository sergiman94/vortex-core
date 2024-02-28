
package com.vortex.vortexdb.traversal.algorithm.steps;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.*;

public class WeightedEdgeStep {

    private final EdgeStep edgeStep;
    private final PropertyKey weightBy;
    private final double defaultWeight;
    private final long sample;

    public WeightedEdgeStep(Vortex g, Directions direction) {
        this(g, direction, ImmutableList.of());
    }

    public WeightedEdgeStep(Vortex g, List<String> labels) {
        this(g, Directions.BOTH, labels);
    }

    public WeightedEdgeStep(Vortex g, Map<String, Object> properties) {
        this(g, Directions.BOTH, ImmutableList.of(), properties);
    }

    public WeightedEdgeStep(Vortex g, Directions direction, List<String> labels) {
        this(g, direction, labels, ImmutableMap.of());
    }

    public WeightedEdgeStep(Vortex g, Directions direction, List<String> labels,
                            Map<String, Object> properties) {
        this(g, direction, labels, properties,
             Long.parseLong(DEFAULT_MAX_DEGREE), 0L, null, 0.0D,
             Long.parseLong(DEFAULT_SAMPLE));
    }

    public WeightedEdgeStep(Vortex g, Directions direction, List<String> labels,
                            Map<String, Object> properties,
                            long maxDegree, long skipDegree,
                            String weightBy, double defaultWeight, long sample) {
        E.checkArgument(sample > 0L || sample == NO_LIMIT,
                        "The sample must be > 0 or == -1, but got: %s",
                        sample);
        E.checkArgument(maxDegree == NO_LIMIT || maxDegree >= sample,
                        "The max degree must be greater than or equal to " +
                        "sample, but got max degree %s and sample %s",
                        maxDegree, sample);

        this.edgeStep = new EdgeStep(g, direction, labels, properties,
                                     maxDegree, skipDegree);
        if (weightBy != null) {
            this.weightBy = g.propertyKey(weightBy);
        } else {
            this.weightBy = null;
        }
        this.defaultWeight = defaultWeight;
        this.sample = sample;
    }

    public EdgeStep step() {
        return this.edgeStep;
    }

    public PropertyKey weightBy() {
        return this.weightBy;
    }

    public double defaultWeight() {
        return this.defaultWeight;
    }

    public long sample() {
        return this.sample;
    }
}
