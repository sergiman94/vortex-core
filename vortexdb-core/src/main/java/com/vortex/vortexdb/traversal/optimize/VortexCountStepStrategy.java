
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.vortexdb.backend.query.Aggregate.AggregateFunc;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class VortexCountStepStrategy
             extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
             implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = -3910433925919057771L;

    private static final VortexCountStepStrategy INSTANCE;

    static {
        INSTANCE = new VortexCountStepStrategy();
    }

    private VortexCountStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalUtil.convAllHasSteps(traversal);

        // Extract CountGlobalStep
        List<CountGlobalStep> steps = TraversalHelper.getStepsOfClass(
                                      CountGlobalStep.class, traversal);
        if (steps.isEmpty()) {
            return;
        }

        // Find VortexStep before count()
        CountGlobalStep<?> originStep = steps.get(0);
        List<Step<?, ?>> originSteps = new ArrayList<>();
        VortexStep<?, ? extends Element> graphStep = null;
        Step<?, ?> step = originStep;
        do {
            if (!(step instanceof CountGlobalStep ||
                  step instanceof GraphStep ||
                  step instanceof IdentityStep ||
                  step instanceof NoOpBarrierStep ||
                  step instanceof CollectingBarrierStep) ||
                 (step instanceof TraversalParent &&
                  TraversalHelper.anyStepRecursively(s -> {
                      return s instanceof SideEffectStep ||
                             s instanceof AggregateStep;
                  }, (TraversalParent) step))) {
                return;
            }
            originSteps.add(step);
            if (step instanceof VortexStep) {
                graphStep = (VortexStep<?, ? extends Element>) step;
                break;
            }
            step = step.getPreviousStep();
        } while (step != null);

        if (graphStep == null) {
            return;
        }

        // Replace with VortexCountStep
        graphStep.queryInfo().aggregate(AggregateFunc.COUNT, null);
        VortexCountStep<?> countStep = new VortexCountStep<>(traversal, graphStep);
        for (Step<?, ?> origin : originSteps) {
            traversal.removeStep(origin);
        }
        traversal.addStep(0, countStep);
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Collections.singleton(VortexStepStrategy.class);
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Collections.singleton(VortexVertexStepStrategy.class);
    }

    public static VortexCountStepStrategy instance() {
        return INSTANCE;
    }
}
