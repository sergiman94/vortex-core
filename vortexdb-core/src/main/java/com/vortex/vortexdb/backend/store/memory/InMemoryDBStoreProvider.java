
package com.vortex.vortexdb.backend.store.memory;

import com.vortex.vortexdb.backend.store.AbstractBackendStoreProvider;
import com.vortex.vortexdb.backend.store.BackendStore;
import com.vortex.vortexdb.backend.store.memory.InMemoryDBStore.InMemoryGraphStore;
import com.vortex.vortexdb.backend.store.memory.InMemoryDBStore.InMemorySchemaStore;
import com.vortex.vortexdb.util.Events;
import com.vortex.vortexdb.backend.store.AbstractBackendStoreProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDBStoreProvider extends AbstractBackendStoreProvider {

    public static final String TYPE = "memory";

    private static Map<String, InMemoryDBStoreProvider> providers = null;

    public static boolean matchType(String type) {
        return TYPE.equalsIgnoreCase(type);
    }

    public static synchronized InMemoryDBStoreProvider instance(String graph) {
        if (providers == null) {
            providers = new ConcurrentHashMap<>();
        }
        if (!providers.containsKey(graph)) {
            InMemoryDBStoreProvider p = new InMemoryDBStoreProvider(graph);
            providers.putIfAbsent(graph, p);
        }
        return providers.get(graph);
    }

    private InMemoryDBStoreProvider(String graph) {
        this.open(graph);
    }

    @Override
    public void open(String graph) {
        super.open(graph);
        /*
         * Memory store need to init some system property,
         * like task related property-keys and vertex-labels.
         * don't notify from store.open() due to task-tx will
         * call it again and cause dead
         */
        this.notifyAndWaitEvent(Events.STORE_INIT);
    }

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new InMemorySchemaStore(this, this.graph(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new InMemoryGraphStore(this, this.graph(), store);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] Vortex-1328: supports backend table version checking
         * [1.1] Vortex-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         * [1.3] #270 & #398: support shard-index and vertex + sortkey prefix,
         *                    also split range table to rangeInt, rangeFloat,
         *                    rangeLong and rangeDouble
         * [1.4] #746: support userdata for indexlabel
         * [1.5] #820: store vertex properties in one column
         * [1.6] #894: encode label id in string index
         * [1.7] #1333: support read frequency for property key
         */
        return "1.7";
    }
}
