package com.vortex.vortexdb;

import com.vortex.vortexdb.analyzer.Analyzer;
import com.vortex.vortexdb.backend.serializer.AbstractSerializer;
import com.vortex.vortexdb.backend.store.BackendFeatures;
import com.vortex.vortexdb.backend.store.BackendStore;
import com.vortex.vortexdb.backend.store.ram.RamTable;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.event.EventHub;
import com.vortex.vortexdb.task.ServerInfoManager;
import com.vortex.vortexdb.type.define.GraphMode;
import com.vortex.vortexdb.type.define.GraphReadMode;
import com.google.common.util.concurrent.RateLimiter;

public interface VortexParams {

    public Vortex graph();
    public String name();
    public GraphMode mode();
    public GraphReadMode readMode();

    public SchemaTransaction schemaTransaction();
    public GraphTransaction systemTransaction();
    public GraphTransaction graphTransaction();

    public GraphTransaction openTransaction();
    public void closeTx();

    public boolean started();
    public boolean closed();
    public boolean initialized();
    public BackendFeatures backendStoreFeatures();

    public BackendStore loadSchemaStore();
    public BackendStore loadGraphStore();
    public BackendStore loadSystemStore();

    public EventHub schemaEventHub();
    public EventHub graphEventHub();
    public EventHub indexEventHub();

    public VortexConfig configuration();

    public ServerInfoManager serverManager();

    public AbstractSerializer serializer();
    public Analyzer analyzer();
    public RateLimiter writeRateLimiter();
    public RateLimiter readRateLimiter();
    public RamTable ramtable();
}
