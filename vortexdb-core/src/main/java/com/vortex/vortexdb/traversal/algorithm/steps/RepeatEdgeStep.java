
package com.vortex.vortexdb.traversal.algorithm.steps;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.type.define.Directions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.vortex.vortexdb.traversal.algorithm.VortexTraverser.DEFAULT_MAX_DEGREE;

public class RepeatEdgeStep extends EdgeStep {

    private int maxTimes = 1;

    public RepeatEdgeStep(Vortex g, Directions direction) {
        this(g, direction, ImmutableList.of());
    }

    public RepeatEdgeStep(Vortex g, Directions direction, int maxTimes) {
        this(g, direction);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(Vortex g, List<String> labels) {
        this(g, Directions.BOTH, labels);
    }

    public RepeatEdgeStep(Vortex g, List<String> labels, int maxTimes) {
        this(g, labels);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(Vortex g, Map<String, Object> properties) {
        this(g, Directions.BOTH, ImmutableList.of(), properties);
    }

    public RepeatEdgeStep(Vortex g, Map<String, Object> properties,
                          int maxTimes) {
        this(g, properties);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(Vortex g, Directions direction,
                          List<String> labels) {
        this(g, direction, labels, ImmutableMap.of());
    }

    public RepeatEdgeStep(Vortex g, Directions direction,
                          List<String> labels, int maxTimes) {
        this(g, direction, labels);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(Vortex g, Directions direction,
                          List<String> labels,
                          Map<String, Object> properties) {
        this(g, direction, labels, properties,
             Long.parseLong(DEFAULT_MAX_DEGREE), 0L, 1);
    }

    public RepeatEdgeStep(Vortex g, Directions direction,
                          List<String> labels,
                          Map<String, Object> properties, int maxTimes) {
        this(g, direction, labels, properties);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(Vortex g, Directions direction,
                          List<String> labels,
                          Map<String, Object> properties, long degree,
                          long skipDegree, int maxTimes) {
        super(g, direction, labels, properties, degree, skipDegree);
        this.maxTimes = maxTimes;
    }

    public int remainTimes() {
        return this.maxTimes;
    }

    public void decreaseTimes() {
        this.maxTimes--;
    }

    public int maxTimes() {
        return this.maxTimes;
    }
}
