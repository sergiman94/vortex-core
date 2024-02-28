
package com.vortex.vortexdb.traversal.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class VortexStepStrategy
             extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
             implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = -2952498905649139719L;

    private static final VortexStepStrategy INSTANCE;

    static {
        INSTANCE = new VortexStepStrategy();
    }

    private VortexStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalUtil.convAllHasSteps(traversal);

        // Extract conditions in GraphStep
        List<GraphStep> steps = TraversalHelper.getStepsOfClass(
                                GraphStep.class, traversal);
        for (GraphStep originStep : steps) {
            VortexStep<?, ?> newStep = new VortexStep<>(originStep);
            TraversalHelper.replaceStep(originStep, newStep, traversal);

            TraversalUtil.extractHasContainer(newStep, traversal);

            // TODO: support order-by optimize
            // TraversalUtil.extractOrder(newStep, traversal);

            TraversalUtil.extractRange(newStep, traversal, false);

            TraversalUtil.extractCount(newStep, traversal);
        }
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Collections.singleton(VortexCountStepStrategy.class);
    }

    public static VortexStepStrategy instance() {
        return INSTANCE;
    }
}
