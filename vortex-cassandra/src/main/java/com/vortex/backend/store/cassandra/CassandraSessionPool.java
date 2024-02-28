package com.vortex.backend.store.cassandra;

import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.store.BackendSession.AbstractBackendSession;
import com.vortex.vortexdb.backend.store.BackendSessionPool;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.util.E;
import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import jnr.ffi.annotations.In;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CassandraSessionPool extends BackendSessionPool {

    private static final int SECOND = 1000;

    private Cluster cluster;
    private final String keyspace;

    public CassandraSessionPool(VortexConfig config,
                                String keyspace, String store) {
        super(config, keyspace + "/" + store);
        this.cluster = null;
        this.keyspace = keyspace;
    }

    @Override
    public synchronized void open() {
        if (this.opened()) {
            throw new BackendException("Please close the old SessionPool " +
                                       "before opening a new one");
        }

        VortexConfig config = this.config();
        // Contact options
        String hosts = config.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_HOST);
        String portString = config.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_PORT);

        int port = Integer.parseInt(portString);

        assert this.cluster == null || this.cluster.isClosed();
        Builder builder = Cluster.builder()
                                 .addContactPoints(hosts.split(","))
                                 .withPort(port);

        // Timeout options
        int connTimeout = config.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_CONN_TIMEOUT);
        int readTimeout = config.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_READ_TIMEOUT);

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(connTimeout * SECOND);
        socketOptions.setReadTimeoutMillis(readTimeout * SECOND);

        builder.withSocketOptions(socketOptions);

        // Credential options
        String username = config.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_USERNAME);
        String password = config.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_PASSWORD);
        if (!username.isEmpty()) {
            builder.withCredentials(username, password);
        }

        // Compression options
        String compression = config.get(com.vortex.backend.store.cassandra.CassandraOptions.CASSANDRA_COMPRESSION);
        builder.withCompression(Compression.valueOf(compression.toUpperCase()));

        this.cluster = builder.build();
    }

    @Override
    public final synchronized boolean opened() {
        return (this.cluster != null && !this.cluster.isClosed());
    }

    protected final synchronized Cluster cluster() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return this.cluster;
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return new Session();
    }

    @Override
    protected synchronized void doClose() {
        if (this.cluster != null && !this.cluster.isClosed()) {
            this.cluster.close();
        }
    }

    public final boolean clusterConnected() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return !this.cluster.isClosed();
    }

    /**
     * The Session class is a wrapper of driver Session
     * Expect every thread hold a its own session(wrapper)
     */
    public final class Session extends AbstractBackendSession {

        private com.datastax.driver.core.Session session;
        private BatchStatement batch;

        public Session() {
            this.session = null;
            this.batch = new BatchStatement(); // LOGGED
        }

        public BatchStatement add(Statement statement) {
            return this.batch.add(statement);
        }

        @Override
        public void rollback() {
            this.batch.clear();
        }

        @Override
        public ResultSet commit() {
            ResultSet rs = this.session.execute(this.batch);
            // Clear batch if execute() successfully (retained if failed)
            this.batch.clear();
            return rs;
        }

        public void commitAsync() {
            Collection<Statement> statements = this.batch.getStatements();

            int count = 0;
            int processors = Math.min(statements.size(), 1023);
            List<ResultSetFuture> results = new ArrayList<>(processors + 1);
            for (Statement s : statements) {
                ResultSetFuture future = this.session.executeAsync(s);
                results.add(future);

                if (++count > processors) {
                    results.forEach(ResultSetFuture::getUninterruptibly);
                    results.clear();
                    count = 0;
                }
            }
            for (ResultSetFuture future : results) {
                future.getUninterruptibly();
            }

            // Clear batch if execute() successfully (retained if failed)
            this.batch.clear();
        }

        public ResultSet query(Statement statement) {
            assert !this.hasChanges();
            return this.execute(statement);
        }

        public ResultSet execute(Statement statement) {
            return this.session.execute(statement);
        }

        public ResultSet execute(String statement) {
            return this.session.execute(statement);
        }

        public ResultSet execute(String statement, Object... args) {
            return this.session.execute(statement, args);
        }

        private void tryOpen() {
            assert this.session == null;
            try {
                this.open();
            } catch (InvalidQueryException ignored) {}
        }

        @Override
        public void open() {
            this.opened = true;
            assert this.session == null;
            this.session = cluster().connect(keyspace());
        }

        @Override
        public boolean opened() {
            if (this.opened && this.session == null) {
                this.tryOpen();
            }
            return this.opened && this.session != null;
        }

        @Override
        public boolean closed() {
            if (!this.opened || this.session == null) {
                return true;
            }
            return this.session.isClosed();
        }

        @Override
        public void close() {
            assert this.closeable();
            if (this.session == null) {
                return;
            }
            this.session.close();
            this.session = null;
        }

        @Override
        public boolean hasChanges() {
            return this.batch.size() > 0;
        }

        public Collection<Statement> statements() {
            return this.batch.getStatements();
        }

        public String keyspace() {
            return CassandraSessionPool.this.keyspace;
        }

        public Metadata metadata() {
            return CassandraSessionPool.this.cluster.getMetadata();
        }

        public int aggregateTimeout() {
            VortexConfig conf = CassandraSessionPool.this.config();
            return conf.get(com.vortex.backend.store.cassandra.CassandraOptions.AGGR_TIMEOUT);
        }
    }
}
