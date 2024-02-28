package com.vortex.backend.store.cassandra;

import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.id.EdgeId;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.id.IdUtil;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendEntryIterator;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.vortexdb.type.define.VortexKeys;
import com.vortex.common.util.E;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CassandraTables {

    public static final String LABEL_INDEX = "label_index";
    public static final String NAME_INDEX = "name_index";

    private static final DataType TYPE_PK = DataType.cint();
    private static final DataType TYPE_SL = DataType.cint(); // VL/EL
    private static final DataType TYPE_IL = DataType.cint();

    private static final DataType TYPE_UD = DataType.map(DataType.text(),
                                                         DataType.text());

    private static final DataType TYPE_ID = DataType.blob();
    private static final DataType TYPE_PROP = DataType.blob();

    private static final DataType TYPE_TTL = DataType.bigint();
    private static final DataType TYPE_EXPIRED_TIME = DataType.bigint();

    private static final long COMMIT_DELETE_BATCH = Query.COMMIT_BATCH;

    public static class Counters extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE = VortexType.COUNTER.string();

        public Counters() {
            super(TABLE);
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.SCHEMA_TYPE, DataType.text()
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap.of(
                    VortexKeys.ID, DataType.counter()
            );

            this.createTable(session, pkeys, ckeys, columns);
        }

        public long getCounter(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                               VortexType type) {
            Clause where = formatEQ(VortexKeys.SCHEMA_TYPE, type.name());
            Select select = QueryBuilder.select(formatKey(VortexKeys.ID))
                                        .from(TABLE);
            select.where(where);
            Row row = session.execute(select).one();
            if (row == null) {
                return 0L;
            } else {
                return row.getLong(formatKey(VortexKeys.ID));
            }
        }

        public void increaseCounter(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                                    VortexType type, long increment) {
            Update update = QueryBuilder.update(TABLE);
            update.with(QueryBuilder.incr(formatKey(VortexKeys.ID), increment));
            update.where(formatEQ(VortexKeys.SCHEMA_TYPE, type.name()));
            session.execute(update);
        }
    }

    public static class VertexLabel extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE = VortexType.VERTEX_LABEL.string();

        public VertexLabel() {
            super(TABLE);
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.ID, TYPE_SL
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap
                    .<VortexKeys, DataType>builder()
                    .put(VortexKeys.NAME, DataType.text())
                    .put(VortexKeys.ID_STRATEGY, DataType.tinyint())
                    .put(VortexKeys.PRIMARY_KEYS, DataType.list(TYPE_PK))
                    .put(VortexKeys.NULLABLE_KEYS, DataType.set(TYPE_PK))
                    .put(VortexKeys.INDEX_LABELS, DataType.set(TYPE_IL))
                    .put(VortexKeys.PROPERTIES, DataType.set(TYPE_PK))
                    .put(VortexKeys.ENABLE_LABEL_INDEX, DataType.cboolean())
                    .put(VortexKeys.USER_DATA, TYPE_UD)
                    .put(VortexKeys.STATUS, DataType.tinyint())
                    .put(VortexKeys.TTL, TYPE_TTL)
                    .put(VortexKeys.TTL_START_TIME, TYPE_PK)
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, NAME_INDEX, VortexKeys.NAME);
        }
    }

    public static class EdgeLabel extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE = VortexType.EDGE_LABEL.string();

        public EdgeLabel() {
            super(TABLE);
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.ID, TYPE_SL
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap
                    .<VortexKeys, DataType>builder()
                    .put(VortexKeys.NAME, DataType.text())
                    .put(VortexKeys.FREQUENCY, DataType.tinyint())
                    .put(VortexKeys.SOURCE_LABEL, TYPE_SL)
                    .put(VortexKeys.TARGET_LABEL, TYPE_SL)
                    .put(VortexKeys.SORT_KEYS, DataType.list(TYPE_PK))
                    .put(VortexKeys.NULLABLE_KEYS, DataType.set(TYPE_PK))
                    .put(VortexKeys.INDEX_LABELS, DataType.set(TYPE_IL))
                    .put(VortexKeys.PROPERTIES, DataType.set(TYPE_PK))
                    .put(VortexKeys.ENABLE_LABEL_INDEX, DataType.cboolean())
                    .put(VortexKeys.USER_DATA, TYPE_UD)
                    .put(VortexKeys.STATUS, DataType.tinyint())
                    .put(VortexKeys.TTL, TYPE_TTL)
                    .put(VortexKeys.TTL_START_TIME, TYPE_PK)
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, NAME_INDEX, VortexKeys.NAME);
        }
    }

    public static class PropertyKey extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE = VortexType.PROPERTY_KEY.string();

        public PropertyKey() {
            super(TABLE);
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.ID, DataType.cint()
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap
                    .<VortexKeys, DataType>builder()
                    .put(VortexKeys.NAME, DataType.text())
                    .put(VortexKeys.DATA_TYPE, DataType.tinyint())
                    .put(VortexKeys.CARDINALITY, DataType.tinyint())
                    .put(VortexKeys.AGGREGATE_TYPE, DataType.tinyint())
                    .put(VortexKeys.WRITE_TYPE, DataType.tinyint())
                    .put(VortexKeys.PROPERTIES, DataType.set(TYPE_PK))
                    .put(VortexKeys.USER_DATA, TYPE_UD)
                    .put(VortexKeys.STATUS, DataType.tinyint())
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, NAME_INDEX, VortexKeys.NAME);
        }
    }

    public static class IndexLabel extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE = VortexType.INDEX_LABEL.string();

        public IndexLabel() {
            super(TABLE);
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.ID, TYPE_IL
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap
                    .<VortexKeys, DataType>builder()
                    .put(VortexKeys.NAME, DataType.text())
                    .put(VortexKeys.BASE_TYPE, DataType.tinyint())
                    .put(VortexKeys.BASE_VALUE, TYPE_SL)
                    .put(VortexKeys.INDEX_TYPE, DataType.tinyint())
                    .put(VortexKeys.FIELDS, DataType.list(TYPE_PK))
                    .put(VortexKeys.USER_DATA, TYPE_UD)
                    .put(VortexKeys.STATUS, DataType.tinyint())
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, NAME_INDEX, VortexKeys.NAME);
        }
    }

    public static class Vertex extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE = VortexType.VERTEX.string();

        public Vertex(String store) {
            super(joinTableName(store, TABLE));
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.ID, TYPE_ID
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap.of(
                    VortexKeys.LABEL, TYPE_SL,
                    VortexKeys.PROPERTIES, DataType.map(TYPE_PK, TYPE_PROP),
                    VortexKeys.EXPIRED_TIME, TYPE_EXPIRED_TIME
            );

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, LABEL_INDEX, VortexKeys.LABEL);
        }

        @Override
        public void insert(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            Insert insert = this.buildInsert(entry);
            session.add(setTtl(insert, entry));
        }

        @Override
        public void append(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            Update append = this.buildAppend(entry);
            session.add(setTtl(append, entry));
        }
    }

    public static class Edge extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE_SUFFIX = VortexType.EDGE.string();

        private final String store;
        private final Directions direction;

        protected Edge(String store, Directions direction) {
            super(joinTableName(store, table(direction)));
            this.store = store;
            this.direction = direction;
        }

        protected String edgesTable(Directions direction) {
            return joinTableName(this.store, table(direction));
        }

        protected Directions direction() {
            return this.direction;
        }

        protected String labelIndexTable() {
            return this.table();
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.OWNER_VERTEX, TYPE_ID
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of(
                    VortexKeys.DIRECTION, DataType.tinyint(),
                    VortexKeys.LABEL, TYPE_SL,
                    VortexKeys.SORT_VALUES, DataType.text(),
                    VortexKeys.OTHER_VERTEX, TYPE_ID
            );
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap.of(
                    VortexKeys.PROPERTIES, DataType.map(TYPE_PK, TYPE_PROP),
                    VortexKeys.EXPIRED_TIME, TYPE_EXPIRED_TIME
            );

            this.createTable(session, pkeys, ckeys, columns);

            /*
             * Only out-edges table needs label index because we query edges
             * by label from out-edges table
             */
            if (this.direction == Directions.OUT) {
                this.createIndex(session, LABEL_INDEX, VortexKeys.LABEL);
            }
        }

        @Override
        public void insert(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            Insert insert = this.buildInsert(entry);
            session.add(setTtl(insert, entry));
        }

        @Override
        public void append(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            Update update = this.buildAppend(entry);
            session.add(setTtl(update, entry));
        }

        @Override
        protected List<VortexKeys> pkColumnName() {
            return ImmutableList.of(VortexKeys.OWNER_VERTEX);
        }

        @Override
        protected List<VortexKeys> idColumnName() {
            return Arrays.asList(EdgeId.KEYS);
        }

        @Override
        protected List<Object> idColumnValue(Id id) {
            EdgeId edgeId;
            if (id instanceof EdgeId) {
                edgeId = (EdgeId) id;
            } else {
                String[] idParts = EdgeId.split(id);
                if (idParts.length == 1) {
                    // Delete edge by label
                    return Arrays.asList((Object[]) idParts);
                }
                id = IdUtil.readString(id.asString());
                edgeId = EdgeId.parse(id.asString());
            }

            E.checkState(edgeId.direction() == this.direction,
                         "Can't query %s edges from %s edges table",
                         edgeId.direction(), this.direction);

            return idColumnValue(edgeId);
        }

        protected final List<Object> idColumnValue(EdgeId edgeId) {
            // TODO: move to Serializer
            List<Object> list = new ArrayList<>(5);
            list.add(IdUtil.writeBinString(edgeId.ownerVertexId()));
            list.add(edgeId.directionCode());
            list.add(edgeId.edgeLabelId().asLong());
            list.add(edgeId.sortValues());
            list.add(IdUtil.writeBinString(edgeId.otherVertexId()));
            return list;
        }

        @Override
        public void delete(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            /*
             * TODO: Delete edge by label
             * Need to implement the framework that can delete with query
             * which contains id or condition.
             */

            // Let super class do delete if not deleting edge by label
            List<Object> idParts = this.idColumnValue(entry.id());
            if (idParts.size() > 1 || entry.columns().size() > 0) {
                super.delete(session, entry);
                return;
            }

            // The only element is label
            this.deleteEdgesByLabel(session, entry.id());
        }

        protected void deleteEdgesByLabel(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                                          Id label) {
            // Edges in edges_in table will be deleted when direction is OUT
            if (this.direction == Directions.IN) {
                return;
            }

            final String OWNER_VERTEX = formatKey(VortexKeys.OWNER_VERTEX);
            final String SORT_VALUES = formatKey(VortexKeys.SORT_VALUES);
            final String OTHER_VERTEX = formatKey(VortexKeys.OTHER_VERTEX);

            // Query edges by label index
            Select select = QueryBuilder.select().from(this.labelIndexTable());
            select.where(formatEQ(VortexKeys.LABEL, label.asLong()));

            ResultSet rs;
            try {
                rs = session.execute(select);
            } catch (DriverException e) {
                throw new BackendException("Failed to query edges " +
                          "with label '%s' for deleting", e, label);
            }

            // Delete edges
            long count = 0L;
            for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
                Row row = it.next();
                Object ownerVertex = row.getObject(OWNER_VERTEX);
                Object sortValues = row.getObject(SORT_VALUES);
                Object otherVertex = row.getObject(OTHER_VERTEX);

                // Delete OUT edges from edges_out table
                session.add(buildDelete(label, ownerVertex, Directions.OUT,
                                        sortValues, otherVertex));
                // Delete IN edges from edges_in table
                session.add(buildDelete(label, otherVertex, Directions.IN,
                                        sortValues, ownerVertex));

                count += 2L;
                if (count >= COMMIT_DELETE_BATCH) {
                    session.commit();
                    count = 0;
                }
            }
            if (count > 0L) {
                session.commit();
            }
        }

        private Delete buildDelete(Id label, Object ownerVertex,
                                   Directions direction, Object sortValues,
                                   Object otherVertex) {
            Delete delete = QueryBuilder.delete().from(edgesTable(direction));
            delete.where(formatEQ(VortexKeys.OWNER_VERTEX, ownerVertex));
            delete.where(formatEQ(VortexKeys.DIRECTION,
                                  EdgeId.directionToCode(direction)));
            delete.where(formatEQ(VortexKeys.LABEL, label.asLong()));
            delete.where(formatEQ(VortexKeys.SORT_VALUES, sortValues));
            delete.where(formatEQ(VortexKeys.OTHER_VERTEX, otherVertex));
            return delete;
        }

        @Override
        protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            // Merge edges into vertex
            // TODO: merge rows before calling row2Entry()

            com.vortex.backend.store.cassandra.CassandraBackendEntry current = (com.vortex.backend.store.cassandra.CassandraBackendEntry) e1;
            com.vortex.backend.store.cassandra.CassandraBackendEntry next = (com.vortex.backend.store.cassandra.CassandraBackendEntry) e2;

            E.checkState(current == null || current.type().isVertex(),
                         "The current entry must be null or VERTEX");
            E.checkState(next != null && next.type().isEdge(),
                         "The next entry must be EDGE");

            long maxSize = BackendEntryIterator.INLINE_BATCH_SIZE;
            if (current != null && current.subRows().size() < maxSize) {
                Object nextVertexId = next.column(VortexKeys.OWNER_VERTEX);
                if (current.id().equals(IdGenerator.of(nextVertexId))) {
                    current.subRow(next.row());
                    return current;
                }
            }

            return this.wrapByVertex(next);
        }

        private com.vortex.backend.store.cassandra.CassandraBackendEntry wrapByVertex(com.vortex.backend.store.cassandra.CassandraBackendEntry edge) {
            assert edge.type().isEdge();
            Object ownerVertex = edge.column(VortexKeys.OWNER_VERTEX);
            E.checkState(ownerVertex != null, "Invalid backend entry");
            Id vertexId = IdGenerator.of(ownerVertex);
            com.vortex.backend.store.cassandra.CassandraBackendEntry vertex = new com.vortex.backend.store.cassandra.CassandraBackendEntry(
                                               VortexType.VERTEX, vertexId);

            vertex.column(VortexKeys.ID, ownerVertex);
            vertex.column(VortexKeys.PROPERTIES, ImmutableMap.of());

            vertex.subRow(edge.row());
            return vertex;
        }

        private static String table(Directions direction) {
            assert direction == Directions.OUT || direction == Directions.IN;
            return direction.type().string() + TABLE_SUFFIX;
        }

        public static com.vortex.backend.store.cassandra.CassandraTable out(String store) {
            return new Edge(store, Directions.OUT);
        }

        public static com.vortex.backend.store.cassandra.CassandraTable in(String store) {
            return new Edge(store, Directions.IN);
        }
    }

    public static class SecondaryIndex extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE = VortexType.SECONDARY_INDEX.string();

        public SecondaryIndex(String store) {
            this(store, TABLE);
        }

        protected SecondaryIndex(String store, String table) {
            super(joinTableName(store, table));
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.FIELD_VALUES, DataType.text()
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of(
                    VortexKeys.INDEX_LABEL_ID, TYPE_IL,
                    VortexKeys.ELEMENT_IDS, TYPE_ID
            );
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap.of(
                    VortexKeys.EXPIRED_TIME, TYPE_EXPIRED_TIME
            );

            this.createTable(session, pkeys, ckeys, columns);
        }

        @Override
        protected List<VortexKeys> idColumnName() {
            return ImmutableList.of(VortexKeys.FIELD_VALUES,
                                    VortexKeys.INDEX_LABEL_ID,
                                    VortexKeys.ELEMENT_IDS);
        }

        @Override
        protected List<VortexKeys> modifiableColumnName() {
            return ImmutableList.of();
        }

        @Override
        public void delete(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            String fieldValues = entry.column(VortexKeys.FIELD_VALUES);
            if (fieldValues != null) {
                super.delete(session, entry);
                return;
            }

            Long indexLabel = entry.column(VortexKeys.INDEX_LABEL_ID);
            if (indexLabel == null) {
                throw new BackendException("SecondaryIndex deletion needs " +
                                           "INDEX_LABEL_ID, but not provided.");
            }

            Select select = QueryBuilder.select().from(this.table());
            select.where(formatEQ(VortexKeys.INDEX_LABEL_ID, indexLabel));
            select.allowFiltering();

            ResultSet rs;
            try {
                rs = session.execute(select);
            } catch (DriverException e) {
                throw new BackendException("Failed to query secondary " +
                          "indexes with index label id '%s' for deleting",
                          indexLabel, e);
            }

            final String FIELD_VALUES = formatKey(VortexKeys.FIELD_VALUES);
            long count = 0L;
            for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
                fieldValues = it.next().get(FIELD_VALUES, String.class);
                Delete delete = QueryBuilder.delete().from(this.table());
                delete.where(formatEQ(VortexKeys.INDEX_LABEL_ID, indexLabel));
                delete.where(formatEQ(VortexKeys.FIELD_VALUES, fieldValues));
                session.add(delete);

                if (++count >= COMMIT_DELETE_BATCH) {
                    session.commit();
                    count = 0L;
                }
            }
        }

        @Override
        public void insert(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "SecondaryIndex insertion is not supported.");
        }

        @Override
        public void append(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            assert entry.columns().size() == 3 || entry.columns().size() == 4;
            Insert insert = this.buildInsert(entry);
            session.add(setTtl(insert, entry));
        }

        @Override
        public void eliminate(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                              com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            assert entry.columns().size() == 3 || entry.columns().size() == 4;
            this.delete(session, entry);
        }
    }

    public static class SearchIndex extends SecondaryIndex {

        public static final String TABLE = VortexType.SEARCH_INDEX.string();

        public SearchIndex(String store) {
            super(store, TABLE);
        }

        @Override
        public void insert(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "SearchIndex insertion is not supported.");
        }
    }

    /**
     * TODO: set field value as key and set element id as value
     */
    public static class UniqueIndex extends SecondaryIndex {

        public static final String TABLE = VortexType.UNIQUE_INDEX.string();

        public UniqueIndex(String store) {
            super(store, TABLE);
        }

        @Override
        public void insert(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "UniqueIndex insertion is not supported.");
        }
    }

    public abstract static class RangeIndex extends com.vortex.backend.store.cassandra.CassandraTable {

        protected RangeIndex(String store, String table) {
            super(joinTableName(store, table));
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.INDEX_LABEL_ID, TYPE_IL
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of(
                    VortexKeys.FIELD_VALUES, this.fieldValuesType(),
                    VortexKeys.ELEMENT_IDS, TYPE_ID
            );
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap.of(
                    VortexKeys.EXPIRED_TIME, TYPE_EXPIRED_TIME
            );

            this.createTable(session, pkeys, ckeys, columns);
        }

        protected DataType fieldValuesType() {
            return DataType.decimal();
        }

        @Override
        protected List<VortexKeys> idColumnName() {
            return ImmutableList.of(VortexKeys.INDEX_LABEL_ID,
                                    VortexKeys.FIELD_VALUES,
                                    VortexKeys.ELEMENT_IDS);
        }

        @Override
        protected List<VortexKeys> modifiableColumnName() {
            return ImmutableList.of();
        }

        @Override
        public void delete(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            Object fieldValues = entry.column(VortexKeys.FIELD_VALUES);
            if (fieldValues != null) {
                super.delete(session, entry);
                return;
            }

            Long indexLabel = entry.column(VortexKeys.INDEX_LABEL_ID);
            if (indexLabel == null) {
                throw new BackendException("Range index deletion " +
                          "needs INDEX_LABEL_ID, but not provided.");
            }

            Delete delete = QueryBuilder.delete().from(this.table());
            delete.where(formatEQ(VortexKeys.INDEX_LABEL_ID, indexLabel));
            session.add(delete);
        }

        @Override
        public void insert(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "RangeIndex insertion is not supported.");
        }

        @Override
        public void append(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            assert entry.columns().size() == 3 || entry.columns().size() == 4;
            Insert insert = this.buildInsert(entry);
            session.add(setTtl(insert, entry));
        }

        @Override
        public void eliminate(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                              com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            assert entry.columns().size() == 3 || entry.columns().size() == 4;
            this.delete(session, entry);
        }
    }

    public static class RangeIntIndex extends RangeIndex {

        public static final String TABLE = VortexType.RANGE_INT_INDEX.string();

        public RangeIntIndex(String store) {
            super(store, TABLE);
        }

        @Override
        protected DataType fieldValuesType() {
            return DataType.cint();
        }
    }

    public static class RangeFloatIndex extends RangeIndex {

        public static final String TABLE = VortexType.RANGE_FLOAT_INDEX.string();

        public RangeFloatIndex(String store) {
            super(store, TABLE);
        }

        @Override
        protected DataType fieldValuesType() {
            return DataType.cfloat();
        }
    }

    public static class RangeLongIndex extends RangeIndex {

        public static final String TABLE = VortexType.RANGE_LONG_INDEX.string();

        public RangeLongIndex(String store) {
            super(store, TABLE);
        }

        @Override
        protected DataType fieldValuesType() {
            // TODO: DataType.varint()
            return DataType.bigint();
        }
    }

    public static class RangeDoubleIndex extends RangeIndex {

        public static final String TABLE = VortexType.RANGE_DOUBLE_INDEX.string();

        public RangeDoubleIndex(String store) {
            super(store, TABLE);
        }

        @Override
        protected DataType fieldValuesType() {
            return DataType.cdouble();
        }
    }

    public static class ShardIndex extends RangeIndex {

        public static final String TABLE = VortexType.SHARD_INDEX.string();

        public ShardIndex(String store) {
            super(store, TABLE);
        }

        @Override
        protected DataType fieldValuesType() {
            return DataType.text();
        }

        @Override
        public void insert(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session,
                           com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "ShardIndex insertion is not supported.");
        }
    }

    public static class Olap extends com.vortex.backend.store.cassandra.CassandraTable {

        public static final String TABLE = VortexType.OLAP.string();

        private Id pkId;

        public Olap(String store, Id id) {
            super(joinTableName(store, joinTableName(TABLE, id.asString())));
            this.pkId = id;
        }

        @Override
        public void init(com.vortex.backend.store.cassandra.CassandraSessionPool.Session session) {
            ImmutableMap<VortexKeys, DataType> pkeys = ImmutableMap.of(
                    VortexKeys.ID, TYPE_ID
            );
            ImmutableMap<VortexKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<VortexKeys, DataType> columns = ImmutableMap.of(
                    VortexKeys.PROPERTY_VALUE, TYPE_PROP
            );

            this.createTable(session, pkeys, ckeys, columns);
        }

        @Override
        protected Iterator<BackendEntry> results2Entries(Query q, ResultSet r) {
            return new com.vortex.backend.store.cassandra.CassandraEntryIterator(r, q, (e1, row) -> {
                com.vortex.backend.store.cassandra.CassandraBackendEntry e2 = row2Entry(q.resultType(), row);
                e2.subId(this.pkId);
                return this.mergeEntries(e1, e2);
            });
        }

        @Override
        public boolean isOlap() {
            return true;
        }
    }

    public static class OlapSecondaryIndex extends SecondaryIndex {

        public static final String TABLE = VortexType.OLAP.string();

        public OlapSecondaryIndex(String store) {
            this(store, TABLE);
        }

        protected OlapSecondaryIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeIntIndex extends RangeIntIndex {

        public static final String TABLE = VortexType.OLAP.string();

        public OlapRangeIntIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeIntIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeLongIndex extends RangeLongIndex {

        public static final String TABLE = VortexType.OLAP.string();

        public OlapRangeLongIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeLongIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeFloatIndex extends RangeFloatIndex {

        public static final String TABLE = VortexType.OLAP.string();

        public OlapRangeFloatIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeFloatIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    public static class OlapRangeDoubleIndex extends RangeDoubleIndex {

        public static final String TABLE = VortexType.OLAP.string();

        public OlapRangeDoubleIndex(String store) {
            this(store, TABLE);
        }

        protected OlapRangeDoubleIndex(String store, String table) {
            super(joinTableName(store, table));
        }
    }

    private static Statement setTtl(BuiltStatement statement,
                                    com.vortex.backend.store.cassandra.CassandraBackendEntry.Row entry) {
        long ttl = entry.ttl();
        if (ttl != 0L) {
            int calcTtl = (int) Math.ceil(ttl / 1000D);
            Using usingTtl = QueryBuilder.ttl(calcTtl);
            if (statement instanceof Insert) {
                ((Insert) statement).using(usingTtl);
            } else {
                assert statement instanceof Update;
                ((Update) statement).using(usingTtl);
            }
        }
        return statement;
    }
}
