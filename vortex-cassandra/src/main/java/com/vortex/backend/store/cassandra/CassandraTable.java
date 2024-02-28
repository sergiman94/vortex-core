package com.vortex.backend.store.cassandra;

import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.page.PageState;
import com.vortex.vortexdb.backend.query.Aggregate;
import com.vortex.vortexdb.backend.query.Condition;
import com.vortex.vortexdb.backend.query.Condition.Relation;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.query.Query.Order;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendTable;
import com.vortex.vortexdb.backend.store.Shard;
import com.vortex.vortexdb.exception.NotFoundException;
import com.vortex.vortexdb.exception.NotSupportException;
import com.vortex.common.iterator.ExtendableIterator;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.vortexdb.util.CopyUtil;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.querybuilder.*;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class CassandraTable
                extends BackendTable<com.vortex.backend.store.cassandra.CassandraSessionPool.Session,
                                     com.vortex.backend.store.cassandra.CassandraBackendEntry.Row> {

    private static final Logger LOG = Log.logger(com.vortex.backend.store.cassandra.CassandraStore.class);
    private static final int MAX_ELEMENTS_IN_CLAUSE = 65535;

    public CassandraTable(String table) {
        super(table);
    }

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                            "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            com.vortex.backend.store.cassandra.CassandraShard spliter = new com.vortex.backend.store.cassandra.CassandraShard(session,
                                                        session.keyspace(),
                                                        this.table());
            return spliter.getSplits(0, splitSize);
        });
    }

    @Override
    public Number queryNumber(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                              Query query) {
        Aggregate aggregate = query.aggregateNotNull();
        Iterator<Number> results = this.query(query, statement -> {
            // Set request timeout to a large value
            int timeout = session.aggregateTimeout();
            statement.setReadTimeoutMillis(timeout * 1000);
            return session.query(statement);
        }, (q, rs) -> {
            Row row = rs.one();
            if (row == null) {
                return IteratorUtils.of(aggregate.defaultValue());
            }
            return IteratorUtils.of(row.getLong(0));
        });
        return aggregate.reduce(results);
    }

    @Override
    public Iterator<BackendEntry> query(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                                        Query query) {
        return this.query(query, session::query, this::results2Entries);
    }

    protected <R> Iterator<R> query(Query query,
                                    Function<Statement, ResultSet> fetcher,
                                    BiFunction<Query, ResultSet, Iterator<R>>
                                    parser) {
        ExtendableIterator<R> rs = new ExtendableIterator<>();

        if (query.limit() == 0L && !query.noLimit()) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
            return rs;
        }

        List<Select> selects = this.query2Select(this.table(), query);
        try {
            for (Select select : selects) {
                ResultSet results = fetcher.apply(select);
                rs.extend(parser.apply(query, results));
            }
        } catch (DriverException e) {
            LOG.debug("Failed to query [{}], detail statement: {}",
                      query, selects, e);
            throw new BackendException("Failed to query [%s]", e, query);
        }

        LOG.debug("Return {} for query {}", rs, query);
        return rs;
    }

    protected List<Select> query2Select(String table, Query query) {
        // Build query
        Selection selection = QueryBuilder.select();

        // Set aggregate
        Aggregate aggregate = query.aggregate();
        if (aggregate != null) {
            if (aggregate.countAll()) {
                selection.countAll();
            } else {
                selection.fcall(aggregate.func().string(), aggregate.column());
            }
        }

        // Set table
        Select select = selection.from(table);

        // NOTE: Cassandra does not support query.offset()
        if (query.offset() != 0) {
            LOG.debug("Query offset is not supported on Cassandra store " +
                      "currently, it will be replaced by [0, offset + limit)");
        }

        // Set order-by
        for (Map.Entry<VortexKeys, Order> order : query.orders().entrySet()) {
            String name = formatKey(order.getKey());
            if (order.getValue() == Order.ASC) {
                select.orderBy(QueryBuilder.asc(name));
            } else {
                assert order.getValue() == Order.DESC;
                select.orderBy(QueryBuilder.desc(name));
            }
        }

        // Is query by id?
        List<Select> ids = this.queryId2Select(query, select);

        if (query.conditionsSize() == 0) {
            // Query only by id
            this.setPageState(query, ids);
            LOG.debug("Query only by id(s): {}", ids);
            return ids;
        } else {
            List<Select> conds = new ArrayList<>(ids.size());
            for (Select id : ids) {
                // Query by condition
                conds.addAll(this.queryCondition2Select(query, id));
            }
            this.setPageState(query, conds);
            LOG.debug("Query by conditions: {}", conds);
            return conds;
        }
    }

    protected void setPageState(Query query, List<Select> selects) {
        if (query.noLimit() && !query.paging()) {
            return;
        }
        for (Select select : selects) {
            int total = (int) query.total();
            if (!query.noLimit()) {
                E.checkArgument(total == query.total(),
                                "Invalid query limit %s", query.limit());
            } else {
                assert total == -1 : total;
            }

            String page = query.page();
            if (page == null) {
                // Set limit
                assert total > 0 : total;
                select.limit(total);
            } else {
                /*
                 * NOTE: the `total` may be -1 when query.noLimit(),
                 * setFetchSize(-1) means the default fetch size will be used.
                 */
                assert total > 0 || total == -1 : total;
                select.setFetchSize(total);

                /*
                 * Can't set limit here `select.limit(total)`
                 * due to it will cause can't get the next page-state.
                 * Also can't set `select.limit(total + 1)` due to it will
                 * cause error "Paging state mismatch" when setPagingState().
                 */

                // It's the first time if page is empty, skip setPagingState
                if (!page.isEmpty()) {
                    byte[] position = PageState.fromString(page).position();
                    try {
                        select.setPagingState(PagingState.fromBytes(position));
                    } catch (PagingStateException e) {
                        throw new BackendException(e);
                    }
                }
            }
        }
    }

    protected List<Select> queryId2Select(Query query, Select select) {
        // Query by id(s)
        if (query.idsSize() == 0) {
            return ImmutableList.of(select);
        }

        List<VortexKeys> nameParts = this.idColumnName();

        List<List<Object>> ids = new ArrayList<>(query.idsSize());
        for (Id id : query.ids()) {
            List<Object> idParts = this.idColumnValue(id);
            if (nameParts.size() != idParts.size()) {
                throw new NotFoundException(
                          "Unsupported ID format: '%s' (should contain %s)",
                          id, nameParts);
            }
            ids.add(idParts);
        }

        // Query only by partition-key
        if (nameParts.size() == 1) {
            List<Object> idList = new ArrayList<>(ids.size());
            for (List<Object> id : ids) {
                assert id.size() == 1;
                idList.add(id.get(0));
            }
            return this.ids2IdSelects(select, nameParts.get(0), idList);
        }

        /*
         * Query by partition-key + clustering-key
         * NOTE: Error if multi-column IN clause include partition key:
         * error: multi-column relations can only be applied to clustering
         * columns when using: select.where(QueryBuilder.in(names, idList));
         * So we use multi-query instead of IN
         */
        List<Select> selects = new ArrayList<>(ids.size());
        for (List<Object> id : ids) {
            assert nameParts.size() == id.size();
            Select idSelect = cloneSelect(select, this.table());
            /*
             * NOTE: concat with AND relation, like:
             * "pk = id and ck1 = v1 and ck2 = v2"
             */
            for (int i = 0, n = nameParts.size(); i < n; i++) {
                idSelect.where(formatEQ(nameParts.get(i), id.get(i)));
            }
            selects.add(idSelect);
        }
        return selects;
    }

    protected Collection<Select> queryCondition2Select(Query query,
                                                       Select select) {
        // Query by conditions
        Collection<Condition> conditions = query.conditions();
        for (Condition condition : conditions) {
            Clause clause = condition2Cql(condition);
            select.where(clause);
            if (Clauses.needAllowFiltering(clause)) {
                select.allowFiltering();
            }
        }
        return ImmutableList.of(select);
    }

    protected Clause condition2Cql(Condition condition) {
        switch (condition.type()) {
            case AND:
                Condition.And and = (Condition.And) condition;
                Clause left = condition2Cql(and.left());
                Clause right = condition2Cql(and.right());
                return Clauses.and(left, right);
            case OR:
                throw new BackendException("Not support OR currently");
            case RELATION:
                Condition.Relation r = (Condition.Relation) condition;
                return relation2Cql(r);
            default:
                final String msg = "Unsupported condition: " + condition;
                throw new AssertionError(msg);
        }
    }

    protected Clause relation2Cql(Relation relation) {
        String key = relation.serialKey().toString();
        Object value = relation.serialValue();

        switch (relation.relation()) {
            case EQ:
                return QueryBuilder.eq(key, value);
            case GT:
                return QueryBuilder.gt(key, value);
            case GTE:
                return QueryBuilder.gte(key, value);
            case LT:
                return QueryBuilder.lt(key, value);
            case LTE:
                return QueryBuilder.lte(key, value);
            case IN:
                return Clauses.in(key, (List<?>) value);
            case CONTAINS_VALUE:
                return QueryBuilder.contains(key, value);
            case CONTAINS_KEY:
                return QueryBuilder.containsKey(key, value);
            case SCAN:
                String[] col = pkColumnName().stream()
                                             .map(pk -> formatKey(pk))
                                             .toArray(String[]::new);
                Shard shard = (Shard) value;
                Object start = QueryBuilder.raw(shard.start());
                Object end = QueryBuilder.raw(shard.end());
                return Clauses.and(
                        QueryBuilder.gte(QueryBuilder.token(col), start),
                        QueryBuilder.lt(QueryBuilder.token(col), end));
            /*
             * Currently we can't support LIKE due to error:
             * "cassandra no viable alternative at input 'like'..."
             */
            // case LIKE:
            //    return QueryBuilder.like(key, value);
            case NEQ:
            default:
                throw new NotSupportException("relation '%s'", relation);
        }
    }

    private List<Select> ids2IdSelects(Select select, VortexKeys key,
                                       List<Object> ids) {
        int size = ids.size();
        List<Select> selects = new ArrayList<>();
        for (int i = 0, j; i < size; i = j) {
            j = Math.min(i + MAX_ELEMENTS_IN_CLAUSE, size);
            Select idSelect = cloneSelect(select, this.table());
            idSelect.where(QueryBuilder.in(formatKey(key), ids.subList(i, j)));
            selects.add(idSelect);
        }
        return selects;
    }

    protected static Select cloneSelect(Select select, String table) {
        // NOTE: there is no Select.clone(), just use copy instead
        return CopyUtil.copy(select, QueryBuilder.select().from(table));
    }

    protected Iterator<BackendEntry> results2Entries(Query q, ResultSet r) {
        return new com.vortex.backend.store.cassandra.CassandraEntryIterator(r, q, (e1, row) -> {
            com.vortex.backend.store.cassandra.CassandraBackendEntry e2 = row2Entry(q.resultType(), row);
            return this.mergeEntries(e1, e2);
        });
    }

    protected static com.vortex.backend.store.cassandra.CassandraBackendEntry row2Entry(VortexType type, Row row) {
        com.vortex.backend.store.cassandra.CassandraBackendEntry entry = new com.vortex.backend.store.cassandra.CassandraBackendEntry(type);

        List<Definition> cols = row.getColumnDefinitions().asList();
        for (Definition col : cols) {
            String name = col.getName();
            VortexKeys key = CassandraTable.parseKey(name);
            Object value = row.getObject(name);
            if (value == null) {
                assert key == VortexKeys.EXPIRED_TIME;
                continue;
            }
            entry.column(key, value);
        }

        return entry;
    }

    protected List<VortexKeys> pkColumnName() {
        return idColumnName();
    }

    protected List<VortexKeys> idColumnName() {
        return ImmutableList.of(VortexKeys.ID);
    }

    protected List<Object> idColumnValue(Id id) {
        return ImmutableList.of(id.asObject());
    }

    protected List<Long> idColumnValue(com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        return ImmutableList.of(entry.id().asLong());
    }

    protected List<VortexKeys> modifiableColumnName() {
        return ImmutableList.of(VortexKeys.PROPERTIES);
    }

    protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
        // Return the next entry (not merged)
        return e2;
    }

    public static final String formatKey(VortexKeys key) {
        return key.name();
    }

    public static final VortexKeys parseKey(String name) {
        return VortexKeys.valueOf(name.toUpperCase());
    }

    public static final Clause formatEQ(VortexKeys key, Object value) {
        return QueryBuilder.eq(formatKey(key), value);
    }

    /**
     * Insert an entire row
     */
    @Override
    public void insert(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                       com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        session.add(this.buildInsert(entry));
    }

    /**
     * Append several elements to the collection column of a row
     */
    @Override
    public void append(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                       com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        session.add(this.buildAppend(entry));
    }

    /**
     * Eliminate several elements from the collection column of a row
     */
    @Override
    public void eliminate(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                          com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        session.add(this.buildEliminate(entry));
    }

    /**
     * Delete an entire row
     */
    @Override
    public void delete(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                       com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        session.add(this.buildDelete(entry));
    }

    protected Insert buildInsert(com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        assert entry.columns().size() > 0;
        Insert insert = QueryBuilder.insertInto(this.table());

        for (Map.Entry<VortexKeys, Object> c : entry.columns().entrySet()) {
            insert.value(formatKey(c.getKey()), c.getValue());
        }
        return insert;
    }

    protected Update buildAppend(com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        List<VortexKeys> idNames = this.idColumnName();
        List<VortexKeys> colNames = this.modifiableColumnName();

        Map<VortexKeys, Object> columns = entry.columns();

        Update update = QueryBuilder.update(table());

        for (VortexKeys key : colNames) {
            if (!columns.containsKey(key)) {
                continue;
            }

            String name = formatKey(key);
            Object value = columns.get(key);

            if (value instanceof Map) {
                update.with(QueryBuilder.putAll(name, (Map<?, ?>) value));
            } else if (value instanceof List) {
                update.with(QueryBuilder.appendAll(name, (List<?>) value));
            } else {
                update.with(QueryBuilder.append(name, value));
            }
        }

        for (VortexKeys idName : idNames) {
            assert columns.containsKey(idName);
            update.where(formatEQ(idName, columns.get(idName)));
        }
        return update;
    }

    protected Update buildEliminate(com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        List<VortexKeys> idNames = this.idColumnName();
        List<VortexKeys> colNames = this.modifiableColumnName();

        Map<VortexKeys, Object> columns = entry.columns();

        // Update by id
        Update update = QueryBuilder.update(table());

        for (VortexKeys key : colNames) {
            /*
             * NOTE: eliminate from map<text, text> should just pass key,
             * if use the following statement:
             * UPDATE vertices SET PROPERTIES=PROPERTIES-{'city':'"Wuhan"'}
             * WHERE LABEL='person' AND PRIMARY_VALUES='josh';
             * it will throw a cassandra exception:
             * Invalid map literal for properties of typefrozen<set<text>>
             */
            if (!columns.containsKey(key)) {
                continue;
            }

            String name = formatKey(key);
            Object value = columns.get(key);
            if (value instanceof Map) {
                @SuppressWarnings("rawtypes")
                Set<?> keySet = ((Map) value).keySet();
                update.with(QueryBuilder.removeAll(name, keySet));
            } else if (value instanceof Set) {
                update.with(QueryBuilder.removeAll(name, (Set<?>) value));
            } else if (value instanceof List) {
                Set<?> keySet = new HashSet<>((List<?>) value);
                update.with(QueryBuilder.removeAll(name, keySet));
            } else {
                update.with(QueryBuilder.remove(name, value));
            }
        }

        for (VortexKeys idName : idNames) {
            assert columns.containsKey(idName);
            update.where(formatEQ(idName, columns.get(idName)));
        }
        return update;
    }

    protected Delete buildDelete(com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        List<VortexKeys> idNames = this.idColumnName();
        Delete delete = QueryBuilder.delete().from(this.table());

        if (entry.columns().isEmpty()) {
            // Delete just by id
            List<Long> idValues = this.idColumnValue(entry);
            assert idNames.size() == idValues.size();

            for (int i = 0, n = idNames.size(); i < n; i++) {
                delete.where(formatEQ(idNames.get(i), idValues.get(i)));
            }
        } else {
            // Delete just by column keys(must be id columns)
            for (VortexKeys idName : idNames) {
                // TODO: should support other filters (like containsKey)
                delete.where(formatEQ(idName, entry.column(idName)));
            }
            /*
             * TODO: delete by id + keys(like index element-ids -- it seems
             * has been replaced by eliminate() method)
             */
        }
        return delete;
    }

    protected void createTable(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                               ImmutableMap<VortexKeys, DataType> partitionKeys,
                               ImmutableMap<VortexKeys, DataType> clusteringKeys,
                               ImmutableMap<VortexKeys, DataType> columns) {

        Create table = SchemaBuilder.createTable(this.table()).ifNotExists();

        for (Map.Entry<VortexKeys, DataType> entry : partitionKeys.entrySet()) {
            table.addPartitionKey(formatKey(entry.getKey()), entry.getValue());
        }
        for (Map.Entry<VortexKeys, DataType> entry : clusteringKeys.entrySet()) {
            table.addClusteringColumn(formatKey(entry.getKey()),
                                      entry.getValue());
        }
        for (Map.Entry<VortexKeys, DataType> entry : columns.entrySet()) {
            table.addColumn(formatKey(entry.getKey()), entry.getValue());
        }

        LOG.debug("Create table: {}", table);
        session.execute(table);
    }

    protected void dropTable(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
        LOG.debug("Drop table: {}", this.table());
        session.execute(SchemaBuilder.dropTable(this.table()).ifExists());
    }

    protected void truncateTable(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
        LOG.debug("Truncate table: {}", this.table());
        session.execute(QueryBuilder.truncate(this.table()));
    }

    protected void createIndex(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                               String indexLabel, VortexKeys column) {
        String indexName = joinTableName(this.table(), indexLabel);
        SchemaStatement index = SchemaBuilder.createIndex(indexName)
                                             .ifNotExists()
                                             .onTable(this.table())
                                             .andColumn(formatKey(column));
        LOG.debug("Create index: {}", index);
        session.execute(index);
    }

    @Override
    public void clear(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
        this.dropTable(session);
    }

    public void truncate(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
        this.truncateTable(session);
    }

    public boolean isOlap() {
        return false;
    }
}
