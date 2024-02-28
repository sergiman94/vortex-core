
package com.vortex.vortexdb.backend.store;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.store.memory.InMemoryDBStoreProvider;
import com.vortex.vortexdb.backend.store.raft.RaftBackendStoreProvider;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BackendProviderFactory {

    private static final Logger LOG = Log.logger(BackendProviderFactory.class);

    private static Map<String, Class<? extends BackendStoreProvider>> providers;

    static {
        providers = new ConcurrentHashMap<>();
    }

    public static BackendStoreProvider open(VortexParams params) {
        VortexConfig config = params.configuration();
        String backend = config.get(CoreOptions.BACKEND).toLowerCase();
        String graph = config.get(CoreOptions.STORE);
        boolean raftMode = config.get(CoreOptions.RAFT_MODE);

        BackendStoreProvider provider = newProvider(config);
        if (raftMode) {
            LOG.info("Opening backend store '{}' in raft mode for graph '{}'",
                     backend, graph);
            provider = new RaftBackendStoreProvider(provider, params);
        }
        provider.open(graph);
        return provider;
    }

    private static BackendStoreProvider newProvider(VortexConfig config) {
        String backend = config.get(CoreOptions.BACKEND).toLowerCase();
        String graph = config.get(CoreOptions.STORE);

        if (InMemoryDBStoreProvider.matchType(backend)) {
            return InMemoryDBStoreProvider.instance(graph);
        }

        Class<? extends BackendStoreProvider> clazz = providers.get(backend);
        BackendException.check(clazz != null,
                               "Not exists BackendStoreProvider: %s", backend);

        assert BackendStoreProvider.class.isAssignableFrom(clazz);
        BackendStoreProvider instance = null;
        try {
            instance = clazz.newInstance();
        } catch (Exception e) {
            throw new BackendException(e);
        }

        BackendException.check(backend.equals(instance.type()),
                               "BackendStoreProvider with type '%s' " +
                               "can't be opened by key '%s'",
                               instance.type(), backend);
        return instance;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void register(String name, String classPath) {
        ClassLoader classLoader = BackendProviderFactory.class.getClassLoader();
        Class<?> clazz = null;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new BackendException(e);
        }

        // Check subclass
        boolean subclass = BackendStoreProvider.class.isAssignableFrom(clazz);
        BackendException.check(subclass, "Class '%s' is not a subclass of " +
                               "class BackendStoreProvider", classPath);

        // Check exists
        BackendException.check(!providers.containsKey(name),
                               "Exists BackendStoreProvider: %s (%s)",
                               name, providers.get(name));

        // Register class
        providers.put(name, (Class) clazz);
    }
}
