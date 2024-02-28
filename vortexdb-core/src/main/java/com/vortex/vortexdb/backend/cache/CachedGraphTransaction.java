
package com.vortex.vortexdb.backend.cache;

import com.vortex.common.config.VortexConfig;
import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.cache.CachedBackendStore.QueryId;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.IdQuery;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.query.QueryResults;
import com.vortex.vortexdb.backend.store.BackendMutation;
import com.vortex.vortexdb.backend.store.BackendStore;
import com.vortex.vortexdb.backend.store.ram.RamTable;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.event.EventHub;
import com.vortex.common.event.EventListener;
import com.vortex.vortexdb.exception.NotSupportException;
import com.vortex.common.iterator.ExtendableIterator;
import com.vortex.common.iterator.ListIterator;
import com.vortex.common.perf.PerfUtil.Watched;
import com.vortex.vortexdb.schema.IndexLabel;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.Events;
import com.google.common.collect.ImmutableSet;
import com.vortex.vortexdb.backend.transaction.GraphTransaction;

import java.lang.reflect.Array;
import java.util.*;

public final class CachedGraphTransaction extends GraphTransaction {

    private final static int MAX_CACHE_PROPS_PER_VERTEX = 10000;
    private final static int MAX_CACHE_EDGES_PER_QUERY = 100;
    private final static float DEFAULT_LEVEL_RATIO = 0.001f;
    private final static long AVG_VERTEX_ENTRY_SIZE = 40L;
    private final static long AVG_EDGE_ENTRY_SIZE = 100L;

    private final Cache<Id, Object> verticesCache;
    private final Cache<Id, Object> edgesCache;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    public CachedGraphTransaction(VortexParams graph, BackendStore store) {
        super(graph, store);

        VortexConfig conf = graph.configuration();

        String type = conf.get(CoreOptions.VERTEX_CACHE_TYPE);
        long capacity = conf.get(CoreOptions.VERTEX_CACHE_CAPACITY);
        int expire = conf.get(CoreOptions.VERTEX_CACHE_EXPIRE);
        this.verticesCache = this.cache("vertex", type, capacity,
                                        AVG_VERTEX_ENTRY_SIZE, expire);

        type = conf.get(CoreOptions.EDGE_CACHE_TYPE);
        capacity = conf.get(CoreOptions.EDGE_CACHE_CAPACITY);
        expire = conf.get(CoreOptions.EDGE_CACHE_EXPIRE);
        this.edgesCache = this.cache("edge", type, capacity,
                                     AVG_EDGE_ENTRY_SIZE, expire);

        this.listenChanges();
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            this.unlistenChanges();
        }
    }

    private Cache<Id, Object> cache(String prefix, String type, long capacity,
                                    long entrySize, long expire) {
        String name = prefix + "-" + this.params().name();
        Cache<Id, Object> cache;
        switch (type) {
            case "l1":
                cache = CacheManager.instance().cache(name, capacity);
                break;
            case "l2":
                long heapCapacity = (long) (DEFAULT_LEVEL_RATIO * capacity);
                cache = CacheManager.instance().levelCache(super.graph(),
                                                           name, heapCapacity,
                                                           capacity, entrySize);
                break;
            default:
                throw new NotSupportException("cache type '%s'", type);
        }
        // Convert the unit from seconds to milliseconds
        cache.expire(expire * 1000L);
        // Enable metrics for graph cache by default
        cache.enableMetrics(true);
        return cache;
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_CLEAR,
                                                  Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.getName())) {
                LOG.debug("Graph {} clear graph cache on event '{}'",
                          this.graph(), event.getName());
                this.clearCache(null, true);
                return true;
            }
            return false;
        };
        this.store().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received graph cache event: {}",
                      this.graph(), event);
            Object[] args = event.getArgs();
            E.checkArgument(args.length > 0 && args[0] instanceof String,
                            "Expect event action argument");
            if (Cache.ACTION_INVALID.equals(args[0])) {
                event.checkArgs(String.class, VortexType.class, Object.class);
                VortexType type = (VortexType) args[1];
                if (type.isVertex()) {
                    // Invalidate vertex cache
                    Object arg2 = args[2];
                    if (arg2 instanceof Id) {
                        Id id = (Id) arg2;
                        this.verticesCache.invalidate(id);
                    } else if (arg2 != null && arg2.getClass().isArray()) {
                        int size = Array.getLength(arg2);
                        for (int i = 0; i < size; i++) {
                            Object id = Array.get(arg2, i);
                            E.checkArgument(id instanceof Id,
                                            "Expect instance of Id in array, " +
                                            "but got '%s'", id.getClass());
                            this.verticesCache.invalidate((Id) id);
                        }
                    } else {
                        E.checkArgument(false,
                                        "Expect Id or Id[], but got: %s",
                                        arg2);
                    }
                } else if (type.isEdge()) {
                    /*
                     * Invalidate edge cache via clear instead of invalidate
                     * because of the cacheKey is QueryId not EdgeId
                     */
                    // this.edgesCache.invalidate(id);
                    this.edgesCache.clear();
                }
                return true;
            } else if (Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, VortexType.class);
                VortexType type = (VortexType) args[1];
                this.clearCache(type, false);
                return true;
            }
            return false;
        };
        EventHub graphEventHub = this.params().graphEventHub();
        if (!graphEventHub.containsListener(Events.CACHE)) {
            graphEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.store().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub graphEventHub = this.params().graphEventHub();
        graphEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    private void notifyChanges(String action, VortexType type, Id[] ids) {
        EventHub graphEventHub = this.params().graphEventHub();
        graphEventHub.notify(Events.CACHE, action, type, ids);
    }

    private void clearCache(VortexType type, boolean notify) {
        if (type == null || type == VortexType.VERTEX) {
            this.verticesCache.clear();
        }
        if (type == null || type == VortexType.EDGE) {
            this.edgesCache.clear();
        }

        if (notify) {
            this.notifyChanges(Cache.ACTION_CLEARED, null, null);
        }
    }

    private boolean enableCacheVertex() {
        return this.verticesCache.capacity() > 0L;
    }

    private boolean enableCacheEdge() {
        return this.edgesCache.capacity() > 0L;
    }

    private boolean needCacheVertex(VortexVertex vertex) {
        return vertex.sizeOfSubProperties() <= MAX_CACHE_PROPS_PER_VERTEX;
    }

    @Override
    @Watched(prefix = "graphcache")
    protected final Iterator<VortexVertex> queryVerticesFromBackend(Query query) {
        if (this.enableCacheVertex() &&
            query.idsSize() > 0 && query.conditionsSize() == 0) {
            return this.queryVerticesByIds((IdQuery) query);
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    @Watched(prefix = "graphcache")
    private Iterator<VortexVertex> queryVerticesByIds(IdQuery query) {
        if (query.idsSize() == 1) {
            Id vertexId = query.ids().iterator().next();
            VortexVertex vertex = (VortexVertex) this.verticesCache.get(vertexId);
            if (vertex != null) {
                if (!vertex.expired()) {
                    return QueryResults.iterator(vertex);
                }
                this.verticesCache.invalidate(vertexId);
            }
            Iterator<VortexVertex> rs = super.queryVerticesFromBackend(query);
            vertex = QueryResults.one(rs);
            if (vertex == null) {
                return QueryResults.emptyIterator();
            }
            if (needCacheVertex(vertex)) {
                this.verticesCache.update(vertex.id(), vertex);
            }
            return QueryResults.iterator(vertex);
        }

        IdQuery newQuery = new IdQuery(VortexType.VERTEX, query);
        List<VortexVertex> vertices = new ArrayList<>();
        for (Id vertexId : query.ids()) {
            VortexVertex vertex = (VortexVertex) this.verticesCache.get(vertexId);
            if (vertex == null) {
                newQuery.query(vertexId);
            } else if (vertex.expired()) {
                newQuery.query(vertexId);
                this.verticesCache.invalidate(vertexId);
            } else {
                vertices.add(vertex);
            }
        }

        // Join results from cache and backend
        ExtendableIterator<VortexVertex> results = new ExtendableIterator<>();
        if (!vertices.isEmpty()) {
            results.extend(vertices.iterator());
        } else {
            // Just use the origin query if find none from the cache
            newQuery = query;
        }

        if (!newQuery.empty()) {
            Iterator<VortexVertex> rs = super.queryVerticesFromBackend(newQuery);
            // Generally there are not too much data with id query
            ListIterator<VortexVertex> listIterator = QueryResults.toList(rs);
            for (VortexVertex vertex : listIterator.list()) {
                // Skip large vertex
                if (needCacheVertex(vertex)) {
                    this.verticesCache.update(vertex.id(), vertex);
                }
            }
            results.extend(listIterator);
        }

        return results;
    }

    @Override
    @Watched(prefix = "graphcache")
    protected final Iterator<VortexEdge> queryEdgesFromBackend(Query query) {
        RamTable ramtable = this.params().ramtable();
        if (ramtable != null && ramtable.matched(query)) {
            return ramtable.query(query);
        }

        if (!this.enableCacheEdge() || query.empty() ||
            query.paging() || query.bigCapacity()) {
            // Query all edges or query edges in paging, don't cache it
            return super.queryEdgesFromBackend(query);
        }

        Id cacheKey = new QueryId(query);
        Object value = this.edgesCache.get(cacheKey);
        @SuppressWarnings("unchecked")
        Collection<VortexEdge> edges = (Collection<VortexEdge>) value;
        if (value != null) {
            for (VortexEdge edge : edges) {
                if (edge.expired()) {
                    this.edgesCache.invalidate(cacheKey);
                    value = null;
                }
            }
        }

        if (value != null) {
            // Not cached or the cache expired
            return edges.iterator();
        }

        Iterator<VortexEdge> rs = super.queryEdgesFromBackend(query);

        /*
         * Iterator can't be cached, caching list instead
         * there may be super node and too many edges in a query,
         * try fetch a few of the head results and determine whether to cache.
         */
        final int tryMax = 1 + MAX_CACHE_EDGES_PER_QUERY;
        assert tryMax > MAX_CACHE_EDGES_PER_QUERY;
        edges = new ArrayList<>(tryMax);
        for (int i = 0; rs.hasNext() && i < tryMax; i++) {
            edges.add(rs.next());
        }

        if (edges.size() == 0) {
            this.edgesCache.update(cacheKey, Collections.emptyList());
        } else if (edges.size() <= MAX_CACHE_EDGES_PER_QUERY) {
            this.edgesCache.update(cacheKey, edges);
        }

        return new ExtendableIterator<>(edges.iterator(), rs);
    }

    @Override
    @Watched(prefix = "graphcache")
    protected final void commitMutation2Backend(BackendMutation... mutations) {
        // Collect changes before commit
        Collection<VortexVertex> updates = this.verticesInTxUpdated();
        Collection<VortexVertex> deletions = this.verticesInTxRemoved();
        Id[] vertexIds = new Id[updates.size() + deletions.size()];
        int vertexOffset = 0;

        int edgesInTxSize = this.edgesInTxSize();

        try {
            super.commitMutation2Backend(mutations);
            // Update vertex cache
            if (this.enableCacheVertex()) {
                for (VortexVertex vertex : updates) {
                    vertexIds[vertexOffset++] = vertex.id();
                    if (needCacheVertex(vertex)) {
                        // Update cache
                        this.verticesCache.updateIfPresent(vertex.id(), vertex);
                    } else {
                        // Skip large vertex
                        this.verticesCache.invalidate(vertex.id());
                    }
                }
            }
        } finally {
            // Update removed vertex in cache whatever success or fail
            if (this.enableCacheVertex()) {
                for (VortexVertex vertex : deletions) {
                    vertexIds[vertexOffset++] = vertex.id();
                    this.verticesCache.invalidate(vertex.id());
                }
                if (vertexOffset > 0) {
                    this.notifyChanges(Cache.ACTION_INVALIDED,
                                       VortexType.VERTEX, vertexIds);
                }
            }

            // Update edge cache if any edges change
            if (edgesInTxSize > 0 && this.enableCacheEdge()) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
                this.notifyChanges(Cache.ACTION_CLEARED, VortexType.EDGE, null);
            }
        }
    }

    @Override
    public final void removeIndex(IndexLabel indexLabel) {
        try {
            super.removeIndex(indexLabel);
        } finally {
            // Update edge cache if needed (any edge-index is deleted)
            if (indexLabel.baseType() == VortexType.EDGE_LABEL) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
                this.notifyChanges(Cache.ACTION_CLEARED, VortexType.EDGE, null);
            }
        }
    }
}
