

package com.vortex.vortexdb.backend.cache;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.*;
import com.vortex.common.config.VortexConfig;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.util.StringEncoding;

import java.util.Iterator;

/**
 * This class is unused now, just for debug or test
 */
public class CachedBackendStore implements BackendStore {

    private BackendStore store = null;
    private Cache<Id, Object> cache = null;

    public CachedBackendStore(BackendStore store) {
        this.store = store;
        this.cache = CacheManager.instance().cache("store-" + store());
        // Set expire 30s
        this.cache.expire(30 * 1000L);
    }

    @Override
    public String store() {
        return this.store.store();
    }

    @Override
    public String database() {
        return this.store.database();
    }

    @Override
    public BackendStoreProvider provider() {
        return this.store.provider();
    }

    @Override
    public void open(VortexConfig config) {
        this.store.open(config);
    }

    @Override
    public void close() {
        this.store.close();
    }

    @Override
    public boolean opened() {
        return this.store.opened();
    }

    @Override
    public void init() {
        this.store.init();
    }

    @Override
    public void clear(boolean clearSpace) {
        this.store.clear(clearSpace);
    }

    @Override
    public boolean initialized() {
        return this.store.initialized();
    }

    @Override
    public void truncate() {
        this.store.truncate();
    }

    @Override
    public void beginTx() {
        this.store.beginTx();
    }

    @Override
    public void commitTx() {
        this.store.commitTx();
    }

    @Override
    public void rollbackTx() {
        this.store.rollbackTx();
    }

    @Override
    public <R> R metadata(VortexType type, String meta, Object[] args) {
        return this.store.metadata(type, meta, args);
    }

    @Override
    public BackendFeatures features() {
        return this.store.features();
    }

    @Override
    public Id nextId(VortexType type) {
        return this.store.nextId(type);
    }

    @Override
    public void increaseCounter(VortexType type, long increment) {
        this.store.increaseCounter(type, increment);
    }

    @Override
    public long getCounter(VortexType type) {
        return this.store.getCounter(type);
    }

    @Override
    public boolean isSchemaStore() {
        return this.store.isSchemaStore();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        // TODO: invalid cache, or set expire time at least
        this.store.mutate(mutation);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<BackendEntry> query(Query query) {
        if (query.empty()) {
            return this.store.query(query);
        }

        QueryId id = new QueryId(query);
        Object result = this.cache.get(id);
        if (result != null) {
            return (Iterator<BackendEntry>) result;
        } else {
            Iterator<BackendEntry> rs = this.store.query(query);
            if (rs.hasNext()) {
                this.cache.update(id, rs);
            }
            return rs;
        }
    }

    @Override
    public Number queryNumber(Query query) {
        return this.store.queryNumber(query);
    }

    /**
     * Query as an Id for cache
     */
    static class QueryId implements Id {

        private String query;
        private int hashCode;

        public QueryId(Query q) {
            this.query = q.toString();
            this.hashCode = q.hashCode();
        }

        @Override
        public IdType type() {
            return IdType.UNKNOWN;
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof QueryId)) {
                return false;
            }
            return this.query.equals(((QueryId) other).query);
        }

        @Override
        public int compareTo(Id o) {
            return this.query.compareTo(o.asString());
        }

        @Override
        public Object asObject() {
            return this.query;
        }

        @Override
        public String asString() {
            return this.query;
        }

        @Override
        public long asLong() {
            // TODO: improve
            return 0L;
        }

        @Override
        public byte[] asBytes() {
            return StringEncoding.encode(this.query);
        }

        @Override
        public String toString() {
            return this.query;
        }

        @Override
        public int length() {
            return this.query.length();
        }
    }
}
