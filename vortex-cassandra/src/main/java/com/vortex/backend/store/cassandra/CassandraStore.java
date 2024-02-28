package com.vortex.backend.store.cassandra;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.serializer.MergeIterator;
import com.vortex.vortexdb.backend.store.*;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.vortexdb.exception.ConnectionException;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class CassandraStore
                extends AbstractBackendStore<com.vortex.backend.store.cassandra.CassandraSessionPool.Session> {

    private static final Logger LOG = Log.logger(CassandraStore.class);

    private static final BackendFeatures FEATURES = new CassandraFeatures();

    private final String store;
    private final String keyspace;

    private final BackendStoreProvider provider;
    // TODO: move to parent class
    private final Map<String, CassandraTable> tables;

    private com.vortex.backend.store.cassandra.CassandraSessionPool sessions;
    private VortexConfig conf;
    private boolean isGraphStore;

    public CassandraStore(final BackendStoreProvider provider,
                          final String keyspace, final String store) {
        E.checkNotNull(keyspace, "keyspace");
        E.checkNotNull(store, "store");

        this.provider = provider;

        this.keyspace = keyspace;
        this.store = store;
        this.tables = new ConcurrentHashMap<>();

        this.sessions = null;
        this.conf = null;

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            CassandraMetrics metrics = this.createMetrics(this.conf,
                                                          this.sessions,
                                                          this.keyspace);
            return metrics.metrics();
        });

        this.registerMetaHandler("compact", (session, meta, args) -> {
            CassandraMetrics metrics = this.createMetrics(this.conf,
                                                          this.sessions,
                                                          this.keyspace);
            return metrics.compact();
        });
    }

    protected CassandraMetrics createMetrics(VortexConfig conf,
                                             com.vortex.backend.store.cassandra.CassandraSessionPool sessions,
                                             String keyspace) {
        return new CassandraMetrics(conf, sessions, keyspace);
    }

    protected void registerTableManager(VortexType type, CassandraTable table) {
        this.registerTableManager(type.string(), table);
    }

    protected void registerTableManager(String name, CassandraTable table) {
        this.tables.put(name, table);
    }

    protected void unregisterTableManager(String name) {
        this.tables.remove(name);
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.keyspace;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public synchronized void open(VortexConfig config) {
        LOG.debug("Store open: {}", this.store);
        E.checkNotNull(config, "config");

        if (this.sessions == null) {
            this.sessions = new com.vortex.backend.store.cassandra.CassandraSessionPool(config, this.keyspace,
                                                     this.store);
        }

        assert this.sessions != null;
        if (!this.sessions.closed()) {
            // TODO: maybe we should throw an exception here instead of ignore
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }
        this.conf = config;
        String graphStore = this.conf.get(CoreOptions.STORE_GRAPH);
        this.isGraphStore = this.store.equals(graphStore);

        // Init cluster
        this.sessions.open();

        // Init a session for current thread
        try {
            LOG.debug("Store connect with keyspace: {}", this.keyspace);
            try {
                this.sessions.session().open();
            } catch (InvalidQueryException e) {
                // TODO: the error message may be changed in different versions
                if (!e.getMessage().contains(String.format(
                    "Keyspace '%s' does not exist", this.keyspace))) {
                    throw e;
                }
                if (this.isSchemaStore()) {
                    LOG.info("Failed to connect keyspace: {}, " +
                             "try to init keyspace later", this.keyspace);
                }
            }
        } catch (Throwable e) {
            try {
                this.sessions.close();
            } catch (Throwable e2) {
                LOG.warn("Failed to close cluster after an error", e2);
            }
            throw new ConnectionException("Failed to connect to Cassandra", e);
        }

        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);
        this.sessions.close();
    }

    @Override
    public boolean opened() {
        this.checkClusterConnected();
        return this.sessions.session().opened();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkOpened();
        com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                        BackendAction item) {
        com.vortex.backend.store.cassandra.CassandraBackendEntry entry = castBackendEntry(item.entry());

        // Check if the entry has no change
        if (!entry.selfChanged() && entry.subRows().isEmpty()) {
            LOG.warn("The entry will be ignored due to no change: {}", entry);
        }

        switch (item.action()) {
            case INSERT:
                // Insert olap vertex
                if (entry.olap()) {
                    this.table(this.olapTableName(entry.subId()))
                        .insert(session, entry.row());
                    break;
                }
                // Insert entry
                if (entry.selfChanged()) {
                    this.table(entry.type()).insert(session, entry.row());
                }
                // Insert sub rows (edges)
                for (com.vortex.backend.store.cassandra.CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).insert(session, row);
                }
                break;
            case DELETE:
                // Delete olap vertex index by index label
                if (entry.olap()) {
                    this.table(this.olapTableName(entry.type()))
                        .delete(session, entry.row());
                    break;
                }
                // Delete entry
                if (entry.selfChanged()) {
                    this.table(entry.type()).delete(session, entry.row());
                }
                // Delete sub rows (edges)
                for (com.vortex.backend.store.cassandra.CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).delete(session, row);
                }
                break;
            case APPEND:
                // Append olap vertex index
                if (entry.olap()) {
                    this.table(this.olapTableName(entry.type()))
                        .append(session, entry.row());
                    break;
                }
                // Append entry
                if (entry.selfChanged()) {
                    this.table(entry.type()).append(session, entry.row());
                }
                // Append sub rows (edges)
                for (com.vortex.backend.store.cassandra.CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).append(session, row);
                }
                break;
            case ELIMINATE:
                // Eliminate entry
                if (entry.selfChanged()) {
                    this.table(entry.type()).eliminate(session, entry.row());
                }
                // Eliminate sub rows (edges)
                for (com.vortex.backend.store.cassandra.CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).eliminate(session, row);
                }
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkOpened();
        VortexType type = CassandraTable.tableType(query);
        String tableName = query.olap() ? this.olapTableName(type) :
                                          type.string();
        CassandraTable table = this.table(tableName);
        Iterator<BackendEntry> entries = table.query(this.session(), query);
        // Merge olap results as needed
        Set<Id> olapPks = query.olapPks();
        if (this.isGraphStore && !olapPks.isEmpty()) {
            List<Iterator<BackendEntry>> iterators = new ArrayList<>();
            for (Id pk : olapPks) {
                Query q = query.copy();
                table = this.table(this.olapTableName(pk));
                iterators.add(table.query(this.session(), q));
            }
            entries = new MergeIterator<>(entries, iterators,
                                          BackendEntry::mergable);
        }
        return entries;
    }

    @Override
    public Number queryNumber(Query query) {
        this.checkOpened();

        CassandraTable table = this.table(CassandraTable.tableType(query));
        return table.queryNumber(this.sessions.session(), query);
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public void init() {
        this.checkClusterConnected();

        // Create keyspace if needed
        if (!this.existsKeyspace()) {
            this.initKeyspace();
        }

        if (this.sessions.session().opened()) {
            // Session has ever been opened.
            LOG.warn("Session has ever been opened(exist keyspace '{}' before)",
                     this.keyspace);
        } else {
            // Open session explicitly to get the exception when it fails
            this.sessions.session().open();
        }

        // Create tables
        this.checkOpened();
        this.initTables();

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        this.checkClusterConnected();

        if (this.existsKeyspace()) {
            if (!clearSpace) {
                this.checkOpened();
                this.clearTables();
            } else {
                this.clearKeyspace();
            }
        }

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public boolean initialized() {
        this.checkClusterConnected();

        if (!this.existsKeyspace()) {
            return false;
        }
        for (CassandraTable table : this.tables()) {
            if (!this.existsTable(table.table())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void truncate() {
        this.checkOpened();

        this.truncateTables();
        LOG.debug("Store truncated: {}", this.store);
    }

    @Override
    public void beginTx() {
        this.checkOpened();

        com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = this.sessions.session();
        if (session.txState() != TxState.CLEAN) {
            LOG.warn("Store {} expect state CLEAN than {} when begin()",
                     this.store, session.txState());
        }
        session.txState(TxState.BEGIN);
    }

    @Override
    public void commitTx() {
        this.checkOpened();

        com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = this.sessions.session();
        if (session.txState() != TxState.BEGIN) {
            LOG.warn("Store {} expect state BEGIN than {} when commit()",
                     this.store, session.txState());
        }

        if (!session.hasChanges()) {
            session.txState(TxState.CLEAN);
            LOG.debug("Store {} has nothing to commit", this.store);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} commit {} statements: {}", this.store,
                      session.statements().size(), session.statements());
        }

        // TODO how to implement tx perfectly?

        // Do update
        session.txState(TxState.COMMITTING);
        try {
            session.commit();
            session.txState(TxState.CLEAN);
        } catch (DriverException e) {
            session.txState(TxState.COMMITT_FAIL);
            LOG.error("Failed to commit statements due to:", e);
            assert session.statements().size() > 0;
            throw new BackendException(
                      "Failed to commit %s statements: '%s'...", e,
                      session.statements().size(),
                      session.statements().iterator().next());
        }
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();

        com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = this.sessions.session();

        // TODO how to implement perfectly?

        if (session.txState() != TxState.COMMITT_FAIL &&
            session.txState() != TxState.CLEAN) {
            LOG.warn("Store {} expect state COMMITT_FAIL/COMMITTING/CLEAN " +
                     "than {} when rollback()", this.store, session.txState());
        }

        session.txState(TxState.ROLLBACKING);
        try {
            session.rollback();
        } finally {
            // Assume batch commit would auto rollback
            session.txState(TxState.CLEAN);
        }
    }

    protected Cluster cluster() {
        return this.sessions.cluster();
    }

    protected boolean existsKeyspace() {
        return this.cluster().getMetadata().getKeyspace(this.keyspace) != null;
    }

    protected boolean existsTable(String table) {
         KeyspaceMetadata keyspace = this.cluster().getMetadata()
                                         .getKeyspace(this.keyspace);
         if (keyspace != null && keyspace.getTable(table) != null) {
             return true;
         }
         return false;
    }

    protected void initKeyspace() {
        Statement stmt = SchemaBuilder.createKeyspace(this.keyspace)
                                      .ifNotExists().with()
                                      .replication(parseReplica(this.conf));
        // Create keyspace with non-keyspace-session
        LOG.debug("Create keyspace: {}", stmt);
        Session session = this.cluster().connect();
        try {
            session.execute(stmt);
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
    }

    private static Map<String, Object> parseReplica(VortexConfig conf) {
        Map<String, Object> replication = new HashMap<>();
        // Replication strategy: SimpleStrategy or NetworkTopologyStrategy
        String strategy = conf.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_STRATEGY);
        replication.put("class", strategy);

        switch (strategy) {
            case "SimpleStrategy":
                List<String> replicas =
                             conf.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_REPLICATION);
                E.checkArgument(replicas.size() == 1,
                                "Individual factor value should be provided " +
                                "with SimpleStrategy for Cassandra");
                int factor = convertFactor(replicas.get(0));
                replication.put("replication_factor", factor);
                break;
            case "NetworkTopologyStrategy":
                // The replicas format is like 'dc1:2,dc2:1'
                Map<String, String> replicaMap =
                            conf.getMap(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_REPLICATION);
                for (Map.Entry<String, String> e : replicaMap.entrySet()) {
                    E.checkArgument(!e.getKey().isEmpty(),
                                    "The datacenter can't be empty");
                    replication.put(e.getKey(), convertFactor(e.getValue()));
                }
                break;
            default:
                throw new AssertionError(String.format(
                          "Illegal replication strategy '%s', valid strategy " +
                          "is 'SimpleStrategy' or 'NetworkTopologyStrategy'",
                          strategy));
        }
        return replication;
    }

    private static int convertFactor(String factor) {
        try {
            return Integer.valueOf(factor);
        } catch (NumberFormatException e) {
            throw new BackendException(
                      "Expect int factor value for SimpleStrategy, " +
                      "but got '%s'", factor);
        }
    }

    protected void clearKeyspace() {
        // Drop keyspace with non-keyspace-session
        Statement stmt = SchemaBuilder.dropKeyspace(this.keyspace).ifExists();
        LOG.debug("Drop keyspace: {}", stmt);

        Session session = this.cluster().connect();
        try {
            session.execute(stmt);
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
    }

    protected void initTables() {
        com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables()) {
            table.clear(session);
        }
    }

    protected void truncateTables() {
        com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables()) {
            if (table.isOlap()) {
                table.dropTable(session);
            } else {
                table.truncate(session);
            }
        }
    }

    protected Collection<CassandraTable> tables() {
        return this.tables.values();
    }

    @Override
    protected final CassandraTable table(VortexType type) {
        return this.table(type.string());
    }

    protected final CassandraTable table(String name) {
        assert name != null;
        CassandraTable table = this.tables.get(name);
        if (table == null) {
            throw new BackendException("Unsupported table: %s", name);
        }
        return table;
    }

    @Override
    protected com.vortex.backend.store.cassandra.CassandraSessionPool.Session session(VortexType type) {
        this.checkOpened();
        return this.sessions.session();
    }

    protected com.vortex.backend.store.cassandra.CassandraSessionPool.Session session() {
        this.checkOpened();
        return this.sessions.session();
    }

    protected final void checkClusterConnected() {
        E.checkState(this.sessions != null && this.sessions.clusterConnected(),
                     "Cassandra cluster has not been connected");
    }

    protected static final com.vortex.backend.store.cassandra.CassandraBackendEntry castBackendEntry(
                                                 BackendEntry entry) {
        assert entry instanceof com.vortex.backend.store.cassandra.CassandraBackendEntry : entry.getClass();
        if (!(entry instanceof com.vortex.backend.store.cassandra.CassandraBackendEntry)) {
            throw new BackendException(
                      "Cassandra store only supports CassandraBackendEntry");
        }
        return (com.vortex.backend.store.cassandra.CassandraBackendEntry) entry;
    }

    /***************************** Store defines *****************************/

    public static class CassandraSchemaStore extends CassandraStore {

        private final CassandraTables.Counters counters;

        public CassandraSchemaStore(BackendStoreProvider provider,
                                    String keyspace, String store) {
            super(provider, keyspace, store);

            this.counters = new CassandraTables.Counters();

            registerTableManager(VortexType.VERTEX_LABEL,
                                 new CassandraTables.VertexLabel());
            registerTableManager(VortexType.EDGE_LABEL,
                                 new CassandraTables.EdgeLabel());
            registerTableManager(VortexType.PROPERTY_KEY,
                                 new CassandraTables.PropertyKey());
            registerTableManager(VortexType.INDEX_LABEL,
                                 new CassandraTables.IndexLabel());
        }

        @Override
        protected Collection<CassandraTable> tables() {
            List<CassandraTable> tables = new ArrayList<>(super.tables());
            tables.add(this.counters);
            return tables;
        }

        @Override
        public void increaseCounter(VortexType type, long increment) {
            this.checkOpened();
            com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = super.sessions.session();
            this.counters.increaseCounter(session, type, increment);
        }

        @Override
        public long getCounter(VortexType type) {
            this.checkOpened();
            com.vortex.backend.store.cassandra.CassandraSessionPool.Session session = super.sessions.session();
            return this.counters.getCounter(session, type);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class CassandraGraphStore extends CassandraStore {

        public CassandraGraphStore(BackendStoreProvider provider,
                                   String keyspace, String store) {
            super(provider, keyspace, store);

            registerTableManager(VortexType.VERTEX,
                                 new CassandraTables.Vertex(store));

            registerTableManager(VortexType.EDGE_OUT,
                                 CassandraTables.Edge.out(store));
            registerTableManager(VortexType.EDGE_IN,
                                 CassandraTables.Edge.in(store));

            registerTableManager(VortexType.SECONDARY_INDEX,
                                 new CassandraTables.SecondaryIndex(store));
            registerTableManager(VortexType.RANGE_INT_INDEX,
                                 new CassandraTables.RangeIntIndex(store));
            registerTableManager(VortexType.RANGE_FLOAT_INDEX,
                                 new CassandraTables.RangeFloatIndex(store));
            registerTableManager(VortexType.RANGE_LONG_INDEX,
                                 new CassandraTables.RangeLongIndex(store));
            registerTableManager(VortexType.RANGE_DOUBLE_INDEX,
                                 new CassandraTables.RangeDoubleIndex(store));
            registerTableManager(VortexType.SEARCH_INDEX,
                                 new CassandraTables.SearchIndex(store));
            registerTableManager(VortexType.SHARD_INDEX,
                                 new CassandraTables.ShardIndex(store));
            registerTableManager(VortexType.UNIQUE_INDEX,
                                 new CassandraTables.UniqueIndex(store));

            registerTableManager(this.olapTableName(VortexType.SECONDARY_INDEX),
                                 new CassandraTables.OlapSecondaryIndex(store));
            registerTableManager(this.olapTableName(VortexType.RANGE_INT_INDEX),
                                 new CassandraTables.OlapRangeIntIndex(store));
            registerTableManager(this.olapTableName(VortexType.RANGE_LONG_INDEX),
                                 new CassandraTables.OlapRangeLongIndex(store));
            registerTableManager(this.olapTableName(VortexType.RANGE_FLOAT_INDEX),
                                 new CassandraTables.OlapRangeFloatIndex(store));
            registerTableManager(this.olapTableName(VortexType.RANGE_DOUBLE_INDEX),
                                 new CassandraTables.OlapRangeDoubleIndex(store));
        }

        @Override
        public Id nextId(VortexType type) {
            throw new UnsupportedOperationException(
                      "CassandraGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(VortexType type, long num) {
            throw new UnsupportedOperationException(
                      "CassandraGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(VortexType type) {
            throw new UnsupportedOperationException(
                      "CassandraGraphStore.getCounter()");
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        /**
         * TODO: can we remove this method since createOlapTable would register?
         */
        @Override
        public void checkAndRegisterOlapTable(Id id) {
            CassandraTable table = new CassandraTables.Olap(this.store(), id);
            if (!this.existsTable(table.table())) {
                throw new VortexException("Not exist table '%s'", table.table());
            }
            registerTableManager(this.olapTableName(id), table);
        }

        @Override
        public void createOlapTable(Id id) {
            CassandraTable table = new CassandraTables.Olap(this.store(), id);
            table.init(this.session());
            registerTableManager(this.olapTableName(id), table);
        }

        @Override
        public void clearOlapTable(Id id) {
            String name = this.olapTableName(id);
            CassandraTable table = this.table(name);
            if (table == null || !this.existsTable(table.table())) {
                throw new VortexException("Not exist table '%s'", name);
            }
            table.truncate(this.session());
        }

        @Override
        public void removeOlapTable(Id id) {
            String name = this.olapTableName(id);
            CassandraTable table = this.table(name);
            if (table == null || !this.existsTable(table.table())) {
                throw new VortexException("Not exist table '%s'", name);
            }
            table.dropTable(this.session());
            this.unregisterTableManager(name);
        }
    }
}
