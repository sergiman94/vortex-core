
package com.vortex.vortexdb.traversal.optimize;

import com.vortex.vortexdb.VortexException;
import org.apache.tinkerpop.gremlin.jsr223.SingleGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Map;

/**
 * ScriptTraversal encapsulates a {@link ScriptEngine} and a script which is compiled into a {@link Traversal} at {@link Admin#applyStrategies()}.
 * This is useful for serializing traversals as the compilation can happen on the remote end where the traversal will ultimately be processed.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VortexScriptTraversal<S, E> extends DefaultTraversal<S, E> {

    private static final long serialVersionUID = 4617322697747299673L;

    private final String script;
    private final String language;
    private final Map<String, Object> bindings;
    private final Map<String, String> aliases;

    private Object result;

    public VortexScriptTraversal(TraversalSource traversalSource,
                                 String language, String script,
                                 Map<String, Object> bindings,
                                 Map<String, String> aliases) {
        this.graph = traversalSource.getGraph();
        this.language = language;
        this.script = script;
        this.bindings = bindings;
        this.aliases = aliases;
        this.result = null;
    }

    public Object result() {
        return this.result;
    }

    public String script() {
        return this.script;
    }

    @Override
    public void applyStrategies() throws IllegalStateException {
        ScriptEngine engine =
                     SingleGremlinScriptEngineManager.get(this.language);

        Bindings bindings = engine.createBindings();
        bindings.putAll(this.bindings);

        @SuppressWarnings("rawtypes")
        TraversalStrategy[] strategies = this.getStrategies().toList()
                                             .toArray(new TraversalStrategy[0]);
        GraphTraversalSource g = this.graph.traversal();
        if (strategies.length > 0) {
            g = g.withStrategies(strategies);
        }
        bindings.put("g", g);
        bindings.put("graph", this.graph);

        for (Map.Entry<String, String> entry : this.aliases.entrySet()) {
            Object value = bindings.get(entry.getValue());
            if (value == null) {
                throw new IllegalArgumentException(String.format(
                          "Invalid aliase '%s':'%s'",
                          entry.getKey(), entry.getValue()));
            }
            bindings.put(entry.getKey(), value);
        }

        try {
            Object result = engine.eval(this.script, bindings);

            if (result instanceof Admin) {
                @SuppressWarnings({ "unchecked", "resource" })
                Admin<S, E> traversal = (Admin<S, E>) result;
                traversal.getSideEffects().mergeInto(this.sideEffects);
                traversal.getSteps().forEach(this::addStep);
                this.strategies = traversal.getStrategies();
            } else {
                this.result = result;
            }
            super.applyStrategies();
        } catch (ScriptException e) {
            throw new VortexException(e.getMessage(), e);
        }
    }
}
