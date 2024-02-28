package com.vortex.vortexdb.backend.store;

import com.vortex.common.config.VortexConfig;
import com.vortex.common.event.EventHub;
import com.vortex.common.event.EventListener;
import com.vortex.common.util.Log;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.store.raft.StoreSnapshotFile;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.vortexdb.util.Events;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import com.vortex.common.util.E;

public abstract class AbstractBackendStoreProvider implements BackendStoreProvider {

    private static final Logger LOG = Log.logger(BackendStoreProvider.class);

    private String graph = null;

    private EventHub storeEventHub = new EventHub("store");

    protected Map<String, BackendStore> stores = null;

    protected final void notifyAndWaitEvent(String event) {
        Future<?> future = this.storeEventHub.notify(event, this);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: {}", event, e);
        }
    }

    protected final void checkOpened() {
        E.checkState(this.graph != null && this.stores != null,
                "The BackendStoreProvider has not been opened");
    }

    protected abstract BackendStore newSchemaStore(String store);

    protected abstract BackendStore newGraphStore(String store);

    @Override
    public void listen(java.util.EventListener listener) {
        this.storeEventHub.listen(EventHub.ANY_EVENT, (EventListener) listener);

    }

    @Override
    public void unlisten(java.util.EventListener listener) {
        this.storeEventHub.unlisten(EventHub.ANY_EVENT, (EventListener) listener);
    }

    @Override
    public String graph() {
        this.checkOpened();
        return this.graph;
    }

    @Override
    public void open(String graph) {
        LOG.debug("Graph '{}' open StoreProvider", this.graph);
        E.checkArgument(graph != null, "The graph name can't be null");
        E.checkArgument(!graph.isEmpty(), "The graph name can't be empty");

        this.graph = graph;
        this.stores = new ConcurrentHashMap<>();

        this.storeEventHub.notify(Events.STORE_OPEN, this);
    }

    @Override
    public void waitStoreStarted() {
        // pass
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("Graph '{}' close StoreProvider", this.graph);
        this.checkOpened();
        this.storeEventHub.notify(Events.STORE_CLOSE, this);
    }

    @Override
    public void init() {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            store.init();
        }
        this.notifyAndWaitEvent(Events.STORE_INIT);

        LOG.debug("Graph '{}' store has been initialized", this.graph);
    }

    @Override
    public void clear() throws BackendException {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            // Just clear tables of store, not clear space
            store.clear(false);
        }
        for (BackendStore store : this.stores.values()) {
            // Only clear space of store
            store.clear(true);
        }
        this.notifyAndWaitEvent(Events.STORE_CLEAR);

        LOG.debug("Graph '{}' store has been cleared", this.graph);
    }

    @Override
    public void truncate() {
        this.checkOpened();
        for (BackendStore store : this.stores.values()) {
            store.truncate();
        }
        this.notifyAndWaitEvent(Events.STORE_TRUNCATE);

        LOG.debug("Graph '{}' store has been truncated", this.graph);
    }

    @Override
    public void initSystemInfo(Vortex graph) {
        this.checkOpened();
        BackendStoreSystemInfo info = graph.backendStoreSystemInfo();
        info.init();
        this.notifyAndWaitEvent(Events.STORE_INITED);

        LOG.debug("Graph '{}' system info has been initialized", this.graph);
    }

    @Override
    public void createSnapshot() {
        String snapshotPrefix = StoreSnapshotFile.SNAPSHOT_DIR;
        for (BackendStore store : this.stores.values()) {
            store.createSnapshot(snapshotPrefix);
        }
    }

    @Override
    public void resumeSnapshot() {
        String snapshotPrefix = StoreSnapshotFile.SNAPSHOT_DIR;
        for (BackendStore store : this.stores.values()) {
            store.resumeSnapshot(snapshotPrefix, true);
        }
    }

    @Override
    public BackendStore loadSchemaStore(final String name) {
        LOG.debug("The '{}' StoreProvider load SchemaStore '{}'",
                this.type(), name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = this.newSchemaStore(name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        LOG.debug("The '{}' StoreProvider load GraphStore '{}'",
                this.type(),  name);

        this.checkOpened();
        if (!this.stores.containsKey(name)) {
            BackendStore s = this.newGraphStore(name);
            this.stores.putIfAbsent(name, s);
        }

        BackendStore store = this.stores.get(name);
        E.checkNotNull(store, "store");
        return store;
    }

    @Override
    public BackendStore loadSystemStore(String name) {
        return this.loadGraphStore(name);
    }

    @Override
    public EventHub storeEventHub() {
        return this.storeEventHub;
    }

    @Override
    public void onCloneConfig(VortexConfig config, String newGraph) {
        config.setProperty(CoreOptions.STORE.name(), newGraph);
    }

    @Override
    public void onDeleteConfig(VortexConfig config) {
        // pass
    }


}
