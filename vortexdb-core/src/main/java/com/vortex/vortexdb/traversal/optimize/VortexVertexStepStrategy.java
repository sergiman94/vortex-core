
package com.vortex.vortexdb.traversal.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

public final class VortexVertexStepStrategy
             extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
             implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 491355700217483162L;

    private static final VortexVertexStepStrategy INSTANCE;

    static {
        INSTANCE = new VortexVertexStepStrategy();
    }

    private VortexVertexStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalUtil.convAllHasSteps(traversal);

        List<VertexStep> steps = TraversalHelper.getStepsOfClass(
                                 VertexStep.class, traversal);

        boolean batchOptimize = false;
        if (!steps.isEmpty()) {
            boolean withPath = VortexVertexStepStrategy.containsPath(traversal);
            boolean withTree = VortexVertexStepStrategy.containsTree(traversal);
            boolean supportIn = TraversalUtil.getGraph(steps.get(0))
                                             .backendStoreFeatures()
                                             .supportsQueryWithInCondition();
            batchOptimize = !withTree && !withPath && supportIn;
        }

        for (VertexStep originStep : steps) {
            VortexVertexStep<?> newStep = batchOptimize ?
                              new VortexVertexStepByBatch<>(originStep) :
                              new VortexVertexStep<>(originStep);
            TraversalHelper.replaceStep(originStep, newStep, traversal);

            TraversalUtil.extractHasContainer(newStep, traversal);

            // TODO: support order-by optimize
            // TraversalUtil.extractOrder(newStep, traversal);

            TraversalUtil.extractRange(newStep, traversal, true);

            TraversalUtil.extractCount(newStep, traversal);
        }
    }

    /**
     * Does a Traversal contain any Path step
     * @param traversal
     * @return the traversal or its parents contain at least one Path step
     */
    protected static boolean containsPath(Traversal.Admin<?, ?> traversal) {
        boolean hasPath = TraversalHelper.getStepsOfClass(
                          PathStep.class, traversal).size() > 0;
        if (hasPath) {
            return true;
        } else if (traversal instanceof EmptyTraversal) {
            return false;
        }

        TraversalParent parent = traversal.getParent();
        return containsPath(parent.asStep().getTraversal());
    }

    /**
     * Does a Traversal contain any Tree step
     * @param traversal
     * @return the traversal or its parents contain at least one Tree step
     */
    protected static boolean containsTree(Traversal.Admin<?, ?> traversal) {
        boolean hasTree = TraversalHelper.getStepsOfClass(
                TreeStep.class, traversal).size() > 0;
        if (hasTree) {
            return true;
        } else if (traversal instanceof EmptyTraversal) {
            return false;
        }

        TraversalParent parent = traversal.getParent();
        return containsTree(parent.asStep().getTraversal());
    }

    public static VortexVertexStepStrategy instance() {
        return INSTANCE;
    }
}
