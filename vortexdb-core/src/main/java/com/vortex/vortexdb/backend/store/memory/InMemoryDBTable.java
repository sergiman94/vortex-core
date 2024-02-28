
package com.vortex.vortexdb.backend.store.memory;

import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.query.*;
import com.vortex.vortexdb.backend.query.Aggregate.AggregateFunc;
import com.vortex.vortexdb.backend.serializer.TextBackendEntry;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendSession;
import com.vortex.vortexdb.backend.store.BackendTable;
import com.vortex.vortexdb.backend.store.Shard;
import com.vortex.vortexdb.exception.NotSupportException;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.common.util.Log;
import com.google.common.base.Strings;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDBTable extends BackendTable<BackendSession,
                                                  TextBackendEntry> {

    private static final Logger LOG = Log.logger(InMemoryDBTable.class);

    protected final Map<Id, BackendEntry> store;
    private final InMemoryShardSpliter shardSpliter;

    public InMemoryDBTable(VortexType type) {
        super(type.name());
        this.store = new ConcurrentHashMap<>();
        this.shardSpliter = new InMemoryShardSpliter(this.table());
    }

    public InMemoryDBTable(VortexType type, Map<Id, BackendEntry> store) {
        super(type.name());
        this.store = store;
        this.shardSpliter = new InMemoryShardSpliter(this.table());
    }

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                            "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            return this.shardSpliter.getSplits(session, splitSize);
        });
    }

    protected Map<Id, BackendEntry> store() {
        return this.store;
    }

    @Override
    public void init(BackendSession session) {
        // pass
    }

    @Override
    public void clear(BackendSession session) {
        this.store.clear();
    }

    @Override
    public void insert(BackendSession session, TextBackendEntry entry) {
        if (!this.store.containsKey(entry.id())) {
            this.store.put(entry.id(), entry);
        } else {
            // Merge columns if the entry exists
            BackendEntry origin = this.store.get(entry.id());
            // TODO: Compatible with BackendEntry
            origin.merge(entry);
        }
    }

    @Override
    public void delete(BackendSession session, TextBackendEntry entry) {
        // Remove by id (TODO: support remove by id + condition)
        this.store.remove(entry.id());
    }

    @Override
    public void append(BackendSession session, TextBackendEntry entry) {
        BackendEntry parent = this.store.get(entry.id());
        if (parent == null) {
            this.store.put(entry.id(), entry);
        } else {
            // TODO: Compatible with BackendEntry
            ((TextBackendEntry) parent).append(entry);
        }
    }

    @Override
    public void eliminate(BackendSession session, TextBackendEntry entry) {
        BackendEntry parent = this.store.get(entry.id());
        // TODO: Compatible with BackendEntry
        if (parent != null) {
            ((TextBackendEntry) parent).eliminate(entry);
        }
    }

    @Override
    public Number queryNumber(BackendSession session, Query query) {
        Aggregate aggregate = query.aggregateNotNull();
        if (aggregate.func() != AggregateFunc.COUNT) {
            throw new NotSupportException(aggregate.toString());
        }

        assert aggregate.func() == AggregateFunc.COUNT;
        Iterator<BackendEntry> results = this.query(session, query);
        long total = 0L;
        while (results.hasNext()) {
            total += this.sizeOfBackendEntry(results.next());
        }
        return total;
    }

    @Override
    public Iterator<BackendEntry> query(BackendSession session, Query query) {
        String page = query.page();
        if (page != null && !page.isEmpty()) {
            throw new NotSupportException("paging by InMemoryDBStore");
        }

        Map<Id, BackendEntry> rs = this.store;

        if (query instanceof IdPrefixQuery) {
            IdPrefixQuery pq = (IdPrefixQuery) query;
            rs = this.queryByIdPrefix(pq.start(), pq.inclusiveStart(),
                                      pq.prefix(), rs);
        }

        if (query instanceof IdRangeQuery) {
            IdRangeQuery rq = (IdRangeQuery) query;
            rs = this.queryByIdRange(rq.start(), rq.inclusiveStart(),
                                     rq.end(), rq.inclusiveEnd(), rs);
        }

        // Query by id(s)
        if (query.idsSize() > 0) {
            rs = this.queryById(query.ids(), rs);
        }

        // Query by condition(s)
        if (query.conditionsSize() > 0) {
            ConditionQuery condQuery = (ConditionQuery) query;
            if (condQuery.containsScanRelation()) {
                return this.queryByRange(condQuery);
            }
            rs = this.queryByFilter(query.conditions(), rs);
        }

        Iterator<BackendEntry> iterator = rs.values().iterator();

        long offset = query.offset() - query.actualOffset();
        if (offset >= rs.size()) {
            query.goOffset(rs.size());
            return QueryResults.emptyIterator();
        }
        if (offset > 0L) {
            query.goOffset(offset);
            iterator = this.skipOffset(iterator, offset);
        }

        if (!query.noLimit() && query.total() < rs.size()) {
            iterator = this.dropTails(iterator, query.limit());
        }
        return iterator;
    }

    private Iterator<BackendEntry> queryByRange(ConditionQuery query) {
        E.checkArgument(query.relations().size() == 1,
                        "Invalid scan with multi conditions: %s", query);
        Condition.Relation scan = query.relations().iterator().next();
        Shard shard = (Shard) scan.value();
        int start = Strings.isNullOrEmpty(shard.start()) ?
                    0 : Long.valueOf(shard.start()).intValue();
        int end = Strings.isNullOrEmpty(shard.end()) ?
                  0 : Long.valueOf(shard.end()).intValue();

        List<BackendEntry> rs = new ArrayList<>(end - start);

        Iterator<BackendEntry> iterator = this.store.values().iterator();
        int i = 0;
        while (iterator.hasNext() && i++ < end) {
            BackendEntry entry = iterator.next();
            if (i > start) {
                rs.add(entry);
            }
        }
        return rs.iterator();
    }

    protected Map<Id, BackendEntry> queryById(Collection<Id> ids,
                                              Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = InsertionOrderUtil.newMap();

        for (Id id : ids) {
            assert !id.number();
            if (entries.containsKey(id)) {
                rs.put(id, entries.get(id));
            }
        }
        return rs;
    }

    protected Map<Id, BackendEntry> queryByIdPrefix(Id start,
                                                    boolean inclusiveStart,
                                                    Id prefix,
                                                    Map<Id, BackendEntry> rs) {
        throw new BackendException("Unsupported prefix query: " + prefix);
    }

    protected Map<Id, BackendEntry> queryByIdRange(Id start,
                                                   boolean inclusiveStart,
                                                   Id end,
                                                   boolean inclusiveEnd,
                                                   Map<Id, BackendEntry> rs) {
        throw new BackendException("Unsupported range query: " + start);
    }

    protected Map<Id, BackendEntry> queryByFilter(
                                    Collection<Condition> conditions,
                                    Map<Id, BackendEntry> entries) {
        assert conditions.size() > 0;

        Map<Id, BackendEntry> rs = new HashMap<>();

        LOG.trace("queryByFilter {} size = {}", this.table(), entries.size());
        for (BackendEntry entry : entries.values()) {
            // Query by conditions
            boolean matched = true;
            for (Condition c : conditions) {
                if (!matchCondition(entry, c)) {
                    // TODO: deal with others Condition like: and, or...
                    matched = false;
                    break;
                }
            }
            if (matched) {
                rs.put(entry.id(), entry);
            }
        }
        return rs;
    }

    protected Iterator<BackendEntry> skipOffset(Iterator<BackendEntry> iterator,
                                                long offset) {
        // Skip offset (TODO: maybe we can improve when adding items to rs)
        for (long i = 0; i < offset && iterator.hasNext(); i++) {
            iterator.next();
        }
        return iterator;
    }

    protected Iterator<BackendEntry> dropTails(Iterator<BackendEntry> iterator,
                                               long limit) {
        E.checkArgument(limit <= Integer.MAX_VALUE,
                        "Limit must be <= 0x7fffffff, but got '%s'", limit);
        List<BackendEntry> entries = new ArrayList<>((int) limit);
        for (long i = 0L; i < limit && iterator.hasNext(); i++) {
            entries.add(iterator.next());
        }
        return entries.iterator();
    }

    protected long sizeOfBackendEntry(BackendEntry entry) {
        return 1L;
    }

    private static boolean matchCondition(BackendEntry item, Condition c) {
        // TODO: Compatible with BackendEntry
        TextBackendEntry entry = (TextBackendEntry) item;

        // Not supported by memory
        if (!(c instanceof Condition.Relation)) {
            throw new BackendException("Unsupported condition: " + c);
        }

        Condition.Relation r = (Condition.Relation) c;
        String key = r.serialKey().toString();

        // TODO: deal with others Relation like: <, >=, ...
        if (r.relation() == Condition.RelationType.CONTAINS_KEY) {
            return entry.contains(r.serialValue().toString());
        } else if (r.relation() == Condition.RelationType.CONTAINS_VALUE) {
            return entry.containsValue(r.serialValue().toString());
        } else if (r.relation() == Condition.RelationType.EQ) {
            return entry.contains(key, r.serialValue().toString());
        } else if (entry.contains(key)) {
            return r.test(entry.column(key));
        }
        return false;
    }

    private class InMemoryShardSpliter extends ShardSpliter<BackendSession> {

        public InMemoryShardSpliter(String table) {
            super(table);
        }

        @Override
        protected long maxKey() {
            return InMemoryDBTable.this.store.size();
        }

        @Override
        protected long estimateDataSize(BackendSession session) {
            return 0L;
        }

        @Override
        protected long estimateNumKeys(BackendSession session) {
            return InMemoryDBTable.this.store.size();
        }
    }
}
