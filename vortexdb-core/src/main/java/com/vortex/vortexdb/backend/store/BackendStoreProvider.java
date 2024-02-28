package com.vortex.vortexdb.backend.store;

import com.vortex.common.config.VortexConfig;
import com.vortex.common.event.EventHub;
import com.vortex.vortexdb.Vortex;

import java.util.EventListener;

public interface BackendStoreProvider {

    // backend store type
    public String type();

    // backend store version
    public String version();

    // graph name (that's database name)
    public String graph();

    public BackendStore loadSystemStore(String name);

    public BackendStore loadSchemaStore(String name);

    public BackendStore loadGraphStore(String name);

    public void open(String name);

    public void waitStoreStarted();

    public void close();

    public void init();

    public void clear();

    public void truncate();

    public void initSystemInfo(Vortex graph);

    public void createSnapshot();

    public void resumeSnapshot();

    public void listen(EventListener listener);

    public void unlisten(EventListener listener);

    public EventHub storeEventHub();

    public void onCloneConfig(VortexConfig config, String newGraph);

    public void onDeleteConfig(VortexConfig config);

}
