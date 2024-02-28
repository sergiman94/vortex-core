
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.NoSuchElementException;
import java.util.Objects;

public final class VortexCountStep<S extends Element>
             extends AbstractStep<S, Long> {

    private static final long serialVersionUID = -679873894532085972L;

    private final VortexStep<?, S> originGraphStep;
    private boolean done = false;

    public VortexCountStep(final Traversal.Admin<?, ?> traversal,
                           final VortexStep<?, S> originGraphStep) {
        super(traversal);
        E.checkNotNull(originGraphStep, "originGraphStep");
        this.originGraphStep = originGraphStep;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.originGraphStep, this.done);
    }

    @Override
    protected Admin<Long> processNextStart() throws NoSuchElementException {
        if (this.done) {
            throw FastNoSuchElementException.instance();
        }
        this.done = true;
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Step<Long, Long> step = (Step) this;
        return this.getTraversal().getTraverserGenerator()
                   .generate(this.originGraphStep.count(), step, 1L);
    }
}
