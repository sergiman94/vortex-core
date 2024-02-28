
package com.vortex.api.api.graph;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.api.api.API;
import com.vortex.common.config.VortexConfig;
import com.vortex.api.config.ServerOptions;
import com.vortex.api.define.Checkable;
import com.vortex.api.define.UpdateStrategy;
import com.vortex.api.metrics.MetricsUtil;
import com.vortex.api.server.RestServer;
import com.vortex.vortexdb.structure.VortexElement;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.codahale.metrics.Meter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchAPI extends API {

    private static final Logger LOG = Log.logger(BatchAPI.class);

    // NOTE: VertexAPI and EdgeAPI should share a counter
    private static final AtomicInteger batchWriteThreads = new AtomicInteger(0);

    static {
        MetricsUtil.registerGauge(RestServer.class, "batch-write-threads",
                                  () -> batchWriteThreads.intValue());
    }

    private final Meter batchMeter;

    public BatchAPI() {
        this.batchMeter = MetricsUtil.registerMeter(this.getClass(),
                                                    "batch-commit");
    }

    public <R> R commit(VortexConfig config, Vortex g, int size,
                        Callable<R> callable) {
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        int writingThreads = batchWriteThreads.incrementAndGet();
        if (writingThreads > maxWriteThreads) {
            batchWriteThreads.decrementAndGet();
            throw new VortexException("The rest server is too busy to write");
        }

        LOG.debug("The batch writing threads is {}", batchWriteThreads);
        try {
            R result = commit(g, callable);
            this.batchMeter.mark(size);
            return result;
        } finally {
            batchWriteThreads.decrementAndGet();
        }
    }

    @JsonIgnoreProperties(value = {"type"})
    protected static abstract class JsonElement implements Checkable {

        @JsonProperty("id")
        public Object id;
        @JsonProperty("label")
        public String label;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonProperty("type")
        public String type;

        @Override
        public abstract void checkCreate(boolean isBatch);

        @Override
        public abstract void checkUpdate();

        protected abstract Object[] properties();
    }

    protected void updateExistElement(JsonElement oldElement,
                                      JsonElement newElement,
                                      Map<String, UpdateStrategy> strategies) {
        if (oldElement == null) {
            return;
        }
        E.checkArgument(newElement != null, "The json element can't be null");

        for (Map.Entry<String, UpdateStrategy> kv : strategies.entrySet()) {
            String key = kv.getKey();
            UpdateStrategy updateStrategy = kv.getValue();
            if (oldElement.properties.get(key) != null &&
                newElement.properties.get(key) != null) {
                Object value = updateStrategy.checkAndUpdateProperty(
                               oldElement.properties.get(key),
                               newElement.properties.get(key));
                newElement.properties.put(key, value);
            } else if (oldElement.properties.get(key) != null &&
                       newElement.properties.get(key) == null) {
                // If new property is null & old is present, use old property
                newElement.properties.put(key, oldElement.properties.get(key));
            }
        }
    }

    protected void updateExistElement(Vortex g,
                                      Element oldElement,
                                      JsonElement newElement,
                                      Map<String, UpdateStrategy> strategies) {
        if (oldElement == null) {
            return;
        }
        E.checkArgument(newElement != null, "The json element can't be null");

        for (Map.Entry<String, UpdateStrategy> kv : strategies.entrySet()) {
            String key = kv.getKey();
            UpdateStrategy updateStrategy = kv.getValue();
            if (oldElement.property(key).isPresent() &&
                newElement.properties.get(key) != null) {
                Object value = updateStrategy.checkAndUpdateProperty(
                               oldElement.property(key).value(),
                               newElement.properties.get(key));
                value = g.propertyKey(key).validValueOrThrow(value);
                newElement.properties.put(key, value);
            } else if (oldElement.property(key).isPresent() &&
                       newElement.properties.get(key) == null) {
                // If new property is null & old is present, use old property
                newElement.properties.put(key, oldElement.value(key));
            }
        }
    }

    protected static void updateProperties(VortexElement element,
                                           JsonElement jsonElement,
                                           boolean append) {
        for (Map.Entry<String, Object> e : jsonElement.properties.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();
            if (append) {
                element.property(key, value);
            } else {
                element.property(key).remove();
            }
        }
    }
}
