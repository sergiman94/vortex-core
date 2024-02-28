
package com.vortex.api.auth;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexFactory;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.StandardVortex;
import com.vortex.vortexdb.auth.EntityManager;
import com.vortex.vortexdb.auth.RelationshipManager;
import com.vortex.vortexdb.auth.SchemaDefine;
import com.vortex.vortexdb.auth.StandardAuthManager;
import com.vortex.vortexdb.backend.cache.CacheManager;
import com.vortex.vortexdb.backend.transaction.AbstractTransaction;
import com.vortex.common.concurrent.LockManager;
import com.vortex.api.license.LicenseVerifier;
import com.vortex.api.metrics.ServerReporter;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.schema.SchemaManager;
import com.vortex.vortexdb.schema.builder.EdgeLabelBuilder;
import com.vortex.vortexdb.schema.builder.IndexLabelBuilder;
import com.vortex.vortexdb.schema.builder.PropertyKeyBuilder;
import com.vortex.vortexdb.schema.builder.VertexLabelBuilder;
import com.vortex.api.serializer.JsonSerializer;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.structure.VortexProperty;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.task.VortexTask;
import com.vortex.vortexdb.task.StandardTaskScheduler;
import com.vortex.vortexdb.task.TaskCallable;
import com.vortex.vortexdb.task.TaskCallable.SysTaskCallable;
import com.vortex.vortexdb.task.TaskManager;
import com.vortex.vortexdb.traversal.optimize.VortexCountStepStrategy;
import com.vortex.vortexdb.traversal.optimize.VortexStepStrategy;
import com.vortex.vortexdb.traversal.optimize.VortexVertexStepStrategy;
import com.vortex.vortexdb.variables.VortexVariables;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.configuration.Configuration;
import sun.reflect.Reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

public final class VortexFactoryAuthProxy {

    public static final String GRAPH_FACTORY =
           "gremlin.graph=com.vortex.vortexdb.auth.VortexFactoryAuthProxy";

    private static final Set<String> PROTECT_METHODS = ImmutableSet.of(
                                                       "instance");

    private static final Map<Vortex, Vortex> graphs = new HashMap<>();

    static {
        VortexAuthProxy.setContext(VortexAuthProxy.Context.admin());
        VortexFactoryAuthProxy.registerPrivateActions();
    }

    public static synchronized Vortex open(Configuration config) {
        /*
         * Inject authentication (replace Vortex with VortexAuthProxy)
         * TODO: Add verify to StandardVortex() to prevent dynamic creation
         */
        Vortex graph = VortexFactory.open(config);
        Vortex proxy = graphs.get(graph);
        if (proxy == null) {
            proxy = new VortexAuthProxy(graph);
            graphs.put(graph, proxy);
        }
        return proxy;
    }

    private static void registerPrivateActions() {
        // Thread
        Reflection.registerFieldsToFilter(Thread.class, "name", "priority", "threadQ", "eetop", "single_step", "daemon", "stillborn", "target", "group", "contextClassLoader", "inheritedAccessControlContext", "threadInitNumber", "threadLocals", "inheritableThreadLocals", "stackSize", "nativeParkEventPointer", "tid", "threadSeqNumber", "threadStatus", "parkBlocker", "blocker", "blockerLock", "EMPTY_STACK_TRACE", "SUBCLASS_IMPLEMENTATION_PERMISSION", "uncaughtExceptionHandler", "defaultUncaughtExceptionHandler", "threadLocalRandomSeed", "threadLocalRandomProbe", "threadLocalRandomSecondarySeed");
        Reflection.registerMethodsToFilter(Thread.class, "exit", "dispatchUncaughtException", "clone", "isInterrupted", "registerNatives", "init", "init", "nextThreadNum", "nextThreadID", "blockedOn", "start0", "isCCLOverridden", "auditSubclass", "dumpThreads", "getThreads", "processQueue", "setPriority0", "stop0", "suspend0", "resume0", "interrupt0", "setNativeName");
        Reflection.registerFieldsToFilter(ThreadLocal.class, "threadLocalHashCode", "nextHashCode", "HASH_INCREMENT");
        Reflection.registerMethodsToFilter(ThreadLocal.class, "access$400", "createInheritedMap", "nextHashCode", "initialValue", "setInitialValue", "getMap", "createMap", "childValue");
        Reflection.registerMethodsToFilter(InheritableThreadLocal.class, "getMap", "createMap", "childValue");

        // Vortex
        Reflection.registerFieldsToFilter(com.vortex.api.auth.StandardAuthenticator.class, "graph");
        Reflection.registerMethodsToFilter(com.vortex.api.auth.StandardAuthenticator.class, "initAdminUser", "inputPassword", "graph");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.ConfigAuthenticator.class, "tokens");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexFactoryAuthProxy.class, "PROTECT_METHODS");
        Reflection.registerMethodsToFilter(com.vortex.api.auth.VortexFactoryAuthProxy.class, "genRegisterPrivateActions", "registerClass", "registerPrivateActions", "registerPrivateActions", "c");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthenticator.User.class, "role", "client");
        Reflection.registerFieldsToFilter(org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser.class, "name");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthProxy.class, "LOG", "vortex", "taskScheduler", "authManager", "contexts", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.api.auth.VortexAuthProxy.class, "lambda$0", "access$3", "access$4", "access$2", "access$5", "resetContext", "logUser", "verifyAdminPermission", "verifyStatusPermission", "verifyPermission", "verifySchemaPermission", "verifySchemaPermission", "verifySchemaPermission", "verifySchemaPermission", "verifyNamePermission", "verifyNameExistsPermission", "verifyElemPermission", "verifyElemPermission", "verifyElemPermission", "verifyElemPermission", "verifyResPermission", "verifyResPermission", "verifyUserPermission", "verifyUserPermission", "verifyUserPermission", "getContextString", "access$6", "access$7", "lambda$1", "lambda$2", "lambda$3", "lambda$4", "lambda$5", "lambda$6", "lambda$7", "lambda$8", "lambda$9", "lambda$10", "lambda$11", "lambda$12", "lambda$13", "lambda$14", "lambda$15", "lambda$16", "lambda$17", "lambda$18", "lambda$19", "lambda$20", "lambda$21", "lambda$22", "lambda$23", "lambda$24", "access$8", "access$9", "access$10", "setContext", "getContext");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthProxy.AuthManagerProxy.class, "authManager", "this$0");
        Reflection.registerMethodsToFilter(com.vortex.api.auth.VortexAuthProxy.AuthManagerProxy.class, "currentUsername", "updateCreator");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthProxy.TaskSchedulerProxy.class, "taskScheduler", "this$0");
        Reflection.registerMethodsToFilter(com.vortex.api.auth.VortexAuthProxy.TaskSchedulerProxy.class, "lambda$0", "lambda$1", "lambda$2", "verifyTaskPermission", "verifyTaskPermission", "verifyTaskPermission", "verifyTaskPermission", "hasTaskPermission");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthProxy.GraphTraversalSourceProxy.class, "this$0");
        Reflection.registerFieldsToFilter(org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.class, "connection", "graph", "strategies", "bytecode");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthProxy.TraversalStrategiesProxy.class, "REST_WOEKER", "serialVersionUID", "strategies", "this$0");
        Reflection.registerMethodsToFilter(com.vortex.api.auth.VortexAuthProxy.TraversalStrategiesProxy.class, "translate");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthProxy.VariablesProxy.class, "variables", "this$0");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthProxy.Context.class, "ADMIN", "user");
        Reflection.registerFieldsToFilter(com.vortex.api.auth.VortexAuthProxy.ContextTask.class, "runner", "context");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.StandardVortex.class, "LOG", "started", "closed", "mode", "variables", "name", "params", "configuration", "schemaEventHub", "graphEventHub", "indexEventHub", "writeRateLimiter", "readRateLimiter", "taskManager", "authManager", "features", "storeProvider", "tx", "ramtable", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.StandardVortex.class, "lambda$0", "access$3", "access$4", "access$2", "access$5", "access$6", "access$7", "waitUntilAllTasksCompleted", "access$8", "loadStoreProvider", "graphTransaction", "schemaTransaction", "openSchemaTransaction", "checkGraphNotClosed", "openSystemTransaction", "openGraphTransaction", "systemTransaction", "access$9", "access$10", "access$11", "access$12", "access$13", "access$14", "access$15", "access$16", "access$17", "access$18", "serializer", "loadSchemaStore", "loadSystemStore", "loadGraphStore", "closeTx", "analyzer", "serverInfoManager", "reloadRamtable", "reloadRamtable", "access$19", "access$20", "access$21");
        Reflection.registerFieldsToFilter(c("com.vortex.vortexdb.StandardVortex$StandardVortexParams"), "graph", "this$0");
        Reflection.registerMethodsToFilter(c("com.vortex.vortexdb.StandardVortex$StandardVortexParams"), "access$1", "graph");
        Reflection.registerFieldsToFilter(c("com.vortex.vortexdb.StandardVortex$TinkerPopTransaction"), "refs", "opened", "transactions", "this$0", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(c("com.vortex.vortexdb.StandardVortex$TinkerPopTransaction"), "lambda$0", "access$3", "access$2", "lambda$1", "graphTransaction", "schemaTransaction", "systemTransaction", "access$1", "setOpened", "doCommit", "verifyOpened", "doRollback", "doClose", "destroyTransaction", "doOpen", "setClosed", "getOrNewTransaction", "access$0", "resetState");
        Reflection.registerFieldsToFilter(org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction.class, "readWriteConsumerInternal", "closeConsumerInternal", "transactionListeners");
        Reflection.registerMethodsToFilter(org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction.class, "doClose", "fireOnCommit", "fireOnRollback", "doReadWrite", "lambda$fireOnRollback$1", "lambda$fireOnCommit$0");
        Reflection.registerFieldsToFilter(org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction.class, "g");
        Reflection.registerMethodsToFilter(org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction.class, "doCommit", "doRollback", "doClose", "doOpen", "fireOnCommit", "fireOnRollback", "doReadWrite");
        Reflection.registerFieldsToFilter(c("com.vortex.vortexdb.StandardVortex$Txs"), "schemaTx", "systemTx", "graphTx", "openedTime", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(c("com.vortex.vortexdb.StandardVortex$Txs"), "access$2", "access$1", "access$0");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.backend.transaction.GraphTransaction.class, "indexTx", "addedVertices", "removedVertices", "addedEdges", "removedEdges", "addedProps", "removedProps", "updatedVertices", "updatedEdges", "updatedOldestProps", "locksTable", "checkCustomVertexExist", "checkAdjacentVertexExist", "lazyLoadAdjacentVertex", "ignoreInvalidEntry", "commitPartOfAdjacentEdges", "batchSize", "pageSize", "verticesCapacity", "edgesCapacity", "$assertionsDisabled", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$IdStrategy");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.backend.transaction.GraphTransaction.class, "lambda$0", "lambda$1", "lambda$2", "lambda$3", "lambda$4", "lambda$5", "lambda$6", "lambda$7", "lambda$8", "lambda$9", "lambda$10", "lambda$11", "lambda$12", "lambda$13", "lambda$14", "lambda$15", "lambda$16", "lambda$17", "lambda$18", "lambda$19", "access$1", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$IdStrategy", "indexTransaction", "indexTransaction", "beforeWrite", "prepareCommit", "verticesInTxSize", "edgesInTxSize", "checkTxVerticesCapacity", "checkTxEdgesCapacity", "verticesInTxUpdated", "verticesInTxRemoved", "removingEdgeOwner", "prepareDeletions", "prepareDeletions", "prepareUpdates", "prepareAdditions", "checkVertexExistIfCustomizedId", "checkAggregateProperty", "checkAggregateProperty", "checkNonnullProperty", "queryEdgesFromBackend", "commitPartOfEdgeDeletions", "optimizeQueries", "checkVertexLabel", "checkId", "queryVerticesFromBackend", "joinTxVertices", "joinTxEdges", "lockForUpdateProperty", "optimizeQuery", "verifyVerticesConditionQuery", "verifyEdgesConditionQuery", "indexQuery", "joinTxRecords", "propertyUpdated", "parseEntry", "traverseByLabel", "reset", "queryVerticesByIds", "filterUnmatchedRecords", "skipOffsetOrStopLimit", "filterExpiredResultFromFromBackend", "queryEdgesByIds", "matchEdgeSortKeys", "rightResultFromIndexQuery");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.backend.transaction.IndexableTransaction.class, "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.backend.transaction.IndexableTransaction.class, "indexTransaction", "commit2Backend", "reset");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.backend.transaction.AbstractTransaction.class, "LOG", "ownerThread", "autoCommit", "closed", "committing", "committing2Backend", "graph", "store", "mutation", "serializer", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.backend.transaction.AbstractTransaction.class, "beforeWrite", "prepareCommit", "params", "mutation", "commit2Backend", "autoCommit", "beforeRead", "afterWrite", "afterRead", "commitMutation2Backend", "checkOwnerThread", "doAction", "store", "reset");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.VortexFactory.class, "LOG", "NAME_REGEX", "graphs");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.VortexFactory.class, "lambda$0");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.schema.SchemaElement.class, "graph", "id", "name", "userdata", "status");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.structure.VortexVertex.class, "EMPTY_SET", "id", "label", "edges", "$assertionsDisabled", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$IdStrategy", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$VortexKeys");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.structure.VortexVertex.class, "$SWITCH_TABLE$com$vortex$vortexdb$type$define$IdStrategy", "newProperty", "newProperty", "tx", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$VortexKeys", "checkIdLength", "onUpdateProperty", "ensureFilledProperties", "clone", "clone");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.structure.VortexElement.class, "EMPTY_MAP", "MAX_PROPERTIES", "graph", "properties", "expiredTime", "removed", "fresh", "propLoaded", "defaultValueUpdated", "$assertionsDisabled", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$Cardinality");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.structure.VortexElement.class, "removed", "addProperty", "newProperty", "tx", "onUpdateProperty", "ensureFilledProperties", "propLoaded", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$Cardinality", "getIdValue", "fresh", "updateToDefaultValueIfNone", "copyProperties");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.structure.VortexEdge.class, "id", "label", "name", "sourceVertex", "targetVertex", "isOutEdge", "$assertionsDisabled", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$VortexKeys", "$SWITCH_TABLE$org$apache$tinkerpop$gremlin$structure$Direction");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.structure.VortexEdge.class, "checkAdjacentVertexExist", "newProperty", "newProperty", "tx", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$VortexKeys", "onUpdateProperty", "ensureFilledProperties", "$SWITCH_TABLE$org$apache$tinkerpop$gremlin$structure$Direction", "clone", "clone");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.structure.VortexProperty.class, "owner", "pkey", "value");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.variables.VortexVariables.class, "LOG", "VARIABLES", "VARIABLE_KEY", "VARIABLE_TYPE", "BYTE_VALUE", "BOOLEAN_VALUE", "INTEGER_VALUE", "LONG_VALUE", "FLOAT_VALUE", "DOUBLE_VALUE", "STRING_VALUE", "LIST", "SET", "TYPES", "params", "graph");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.variables.VortexVariables.class, "createPropertyKey", "queryAllVariableVertices", "queryVariableVertex", "createVariableVertex", "removeVariableVertex", "extractSingleObject", "setProperty");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.schema.SchemaManager.class, "transaction", "graph");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.schema.SchemaManager.class, "lambda$0", "lambda$1", "lambda$2", "lambda$3", "checkExists");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.schema.builder.PropertyKeyBuilder.class, "id", "name", "dataType", "cardinality", "aggregateType", "checkExist", "userdata", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.schema.builder.PropertyKeyBuilder.class, "lambda$0", "checkStableVars", "checkAggregateType", "hasSameProperties");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.schema.builder.AbstractBuilder.class, "transaction", "graph");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.schema.builder.AbstractBuilder.class, "rebuildIndex", "graph", "checkSchemaName", "validOrGenerateId", "lockCheckAndCreateSchema", "propertyKeyOrNull", "checkSchemaIdIfRestoringMode", "vertexLabelOrNull", "edgeLabelOrNull", "indexLabelOrNull", "updateSchemaStatus");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.schema.builder.VertexLabelBuilder.class, "id", "name", "idStrategy", "properties", "primaryKeys", "nullableKeys", "ttl", "ttlStartTime", "enableLabelIndex", "userdata", "checkExist", "$assertionsDisabled", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$Action", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$IdStrategy");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.schema.builder.VertexLabelBuilder.class, "lambda$0", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$IdStrategy", "checkStableVars", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$Action", "checkProperties", "checkNullableKeys", "checkIdStrategy", "checkPrimaryKeys", "hasSameProperties", "checkTtl", "checkUserdata", "mapPkId2Name", "mapPkId2Name");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.schema.builder.EdgeLabelBuilder.class, "id", "name", "sourceLabel", "targetLabel", "frequency", "properties", "sortKeys", "nullableKeys", "ttl", "ttlStartTime", "enableLabelIndex", "userdata", "checkExist", "$assertionsDisabled", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$Action");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.schema.builder.EdgeLabelBuilder.class, "lambda$0", "checkStableVars", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$Action", "checkProperties", "checkNullableKeys", "checkSortKeys", "checkRelation", "hasSameProperties", "checkTtl", "checkUserdata", "mapPkId2Name", "mapPkId2Name");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.schema.builder.IndexLabelBuilder.class, "id", "name", "baseType", "baseValue", "indexType", "indexFields", "userdata", "checkExist", "rebuild", "$assertionsDisabled", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$DataType", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$IndexType");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.schema.builder.IndexLabelBuilder.class, "lambda$0", "checkStableVars", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$DataType", "$SWITCH_TABLE$com$vortex$vortexdb$type$define$IndexType", "checkBaseType", "checkIndexType", "checkFields4Range", "loadElement", "checkFields", "checkRepeatIndex", "checkRepeatIndex", "checkRepeatIndex", "checkPrimaryKeyIndex", "checkRepeatRangeIndex", "checkRepeatSearchIndex", "checkRepeatSecondaryIndex", "checkRepeatShardIndex", "checkRepeatUniqueIndex", "removeSubIndex", "hasSubIndex", "allStringIndex", "oneNumericField", "hasSameProperties");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.task.TaskManager.class, "LOG", "SCHEDULE_PERIOD", "THREADS", "MANAGER", "schedulers", "taskExecutor", "taskDbExecutor", "serverInfoDbExecutor", "schedulerExecutor", "contexts", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.task.TaskManager.class, "lambda$0", "resetContext", "closeTaskTx", "setContext", "instance", "closeSchedulerTx", "notifyNewTask", "scheduleOrExecuteJob", "scheduleOrExecuteJobForGraph");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.task.StandardTaskScheduler.class, "LOG", "graph", "serverManager", "taskExecutor", "taskDbExecutor", "eventListener", "tasks", "taskTx", "NO_LIMIT", "PAGE_SIZE", "QUERY_INTERVAL", "MAX_PENDING_TASKS", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.task.StandardTaskScheduler.class, "lambda$0", "lambda$1", "lambda$2", "lambda$3", "lambda$4", "lambda$5", "lambda$6", "lambda$7", "tx", "listenChanges", "unlistenChanges", "submitTask", "queryTask", "queryTask", "queryTask", "call", "call", "remove", "sleep", "taskDone", "serverManager", "supportsPaging", "restore", "checkOnMasterNode", "waitUntilTaskCompleted", "scheduleTasks", "executeTasksOnWorker", "cancelTasksOnWorker");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.task.VortexTask.class, "LOG", "DECOMPRESS_RATIO", "scheduler", "callable", "type", "name", "id", "parent", "dependencies", "description", "context", "create", "server", "load", "status", "progress", "update", "retries", "input", "result", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.task.VortexTask.class, "property", "scheduler", "scheduler", "asArray", "checkPropertySize", "checkPropertySize", "checkDependenciesSuccess", "toOrderSet", "done", "callable", "setException", "set", "result", "status");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.task.TaskCallable.class, "LOG", "ERROR_COMMIT", "ERROR_MESSAGES", "task", "graph", "lastSaveTime", "saveInterval");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.task.TaskCallable.class, "graph", "closeTx", "cancelled", "done", "task", "save", "needSaveWithEx");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.task.TaskCallable.SysTaskCallable.class, "params");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.task.TaskCallable.SysTaskCallable.class, "params", "params");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.auth.StandardAuthManager.class, "CACHE_EXPIRE", "graph", "eventListener", "usersCache", "users", "groups", "targets", "belong", "access", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.auth.StandardAuthManager.class, "lambda$0", "listenChanges", "unlistenChanges", "invalidCache", "initSchemaIfNeeded", "rolePermission", "rolePermission", "rolePermission", "cache");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.auth.SchemaDefine.class, "graph", "label");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.auth.SchemaDefine.class, "schema", "createPropertyKey", "createPropertyKey", "createPropertyKey", "existEdgeLabel", "createRangeIndex", "unhideField", "hideField", "existVertexLabel", "initProperties");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.auth.EntityManager.class, "graph", "label", "deser", "NO_LIMIT", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.auth.EntityManager.class, "toList", "graph", "tx", "commitOrRollback", "unhideLabel", "queryById", "queryEntity", "constructVertex", "save", "query");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.auth.RelationshipManager.class, "graph", "label", "deser", "NO_LIMIT", "$assertionsDisabled");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.auth.RelationshipManager.class, "lambda$0", "toList", "graph", "tx", "commitOrRollback", "unhideLabel", "queryById", "queryRelationship", "newVertex", "save");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.backend.cache.CacheManager.class, "LOG", "INSTANCE", "TIMER_TICK_PERIOD", "LOG_TICK_COST_TIME", "caches", "timer");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.backend.cache.CacheManager.class, "access$0", "scheduleTimer", "instance");
        Reflection.registerFieldsToFilter(com.vortex.common.concurrent.LockManager.class, "INSTANCE", "lockGroupMap");
        Reflection.registerMethodsToFilter(com.vortex.common.concurrent.LockManager.class, "instance");
        Reflection.registerFieldsToFilter(com.vortex.api.license.LicenseVerifier.class, "LOG", "LICENSE_PARAM_PATH", "INSTANCE", "CHECK_INTERVAL", "lastCheckTime", "verifyParam", "manager");
        Reflection.registerMethodsToFilter(com.vortex.api.license.LicenseVerifier.class, "buildVerifyParam", "initLicenseParam", "verifyPublicCert", "instance");
        Reflection.registerFieldsToFilter(com.vortex.api.metrics.ServerReporter.class, "instance", "gauges", "counters", "histograms", "meters", "timers");
        Reflection.registerMethodsToFilter(com.vortex.api.metrics.ServerReporter.class, "instance", "instance");
        Reflection.registerFieldsToFilter(com.codahale.metrics.ScheduledReporter.class, "LOG", "FACTORY_ID", "registry", "executor", "shutdownExecutorOnStop", "disabledMetricAttributes", "scheduledFuture", "filter", "durationFactor", "durationUnit", "rateFactor", "rateUnit");
        Reflection.registerMethodsToFilter(com.codahale.metrics.ScheduledReporter.class, "convertDuration", "convertRate", "getRateUnit", "getDurationUnit", "isShutdownExecutorOnStop", "getDisabledMetricAttributes", "calculateRateUnit", "createDefaultExecutor", "lambda$start$0", "start");
        Reflection.registerFieldsToFilter(com.vortex.api.serializer.JsonSerializer.class, "LBUF_SIZE", "INSTANCE");
        Reflection.registerMethodsToFilter(com.vortex.api.serializer.JsonSerializer.class, "writeIterator", "instance");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.traversal.optimize.VortexVertexStepStrategy.class, "serialVersionUID", "INSTANCE");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.traversal.optimize.VortexVertexStepStrategy.class, "instance");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.traversal.optimize.VortexStepStrategy.class, "serialVersionUID", "INSTANCE");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.traversal.optimize.VortexStepStrategy.class, "instance");
        Reflection.registerFieldsToFilter(com.vortex.vortexdb.traversal.optimize.VortexCountStepStrategy.class, "serialVersionUID", "INSTANCE");
        Reflection.registerMethodsToFilter(com.vortex.vortexdb.traversal.optimize.VortexCountStepStrategy.class, "lambda$0", "instance");

        // Enable this line to generate registration statement
        //genRegisterPrivateActions();
    }

    @SuppressWarnings("unused")
    private static void genRegisterPrivateActions() {
        registerPrivateActions(Thread.class);
        registerPrivateActions(ThreadLocal.class);
        registerPrivateActions(InheritableThreadLocal.class);

        registerPrivateActions(StandardAuthenticator.class);
        registerPrivateActions(ConfigAuthenticator.class);
        registerPrivateActions(VortexFactoryAuthProxy.class);
        registerPrivateActions(VortexAuthenticator.User.class);

        registerPrivateActions(VortexAuthProxy.class);
        registerPrivateActions(VortexAuthProxy.AuthManagerProxy.class);
        registerPrivateActions(VortexAuthProxy.TaskSchedulerProxy.class);
        registerPrivateActions(VortexAuthProxy.GraphTraversalSourceProxy.class);
        registerPrivateActions(VortexAuthProxy.TraversalStrategiesProxy.class);
        registerPrivateActions(VortexAuthProxy.VariablesProxy.class);
        registerPrivateActions(VortexAuthProxy.Context.class);
        registerPrivateActions(VortexAuthProxy.ContextThreadPoolExecutor.class);
        registerPrivateActions(VortexAuthProxy.ContextTask.class);

        for (Class<?> clazz : StandardVortex.PROTECT_CLASSES) {
            registerPrivateActions(clazz);
        }

        registerPrivateActions(VortexFactory.class);
        registerPrivateActions(AbstractTransaction.class);

        registerPrivateActions(SchemaElement.class);
        registerPrivateActions(VortexVertex.class);
        registerPrivateActions(VortexEdge.class);
        registerPrivateActions(VortexProperty.class);
        registerPrivateActions(VortexVariables.class);

        registerPrivateActions(SchemaManager.class);
        registerPrivateActions(PropertyKeyBuilder.class);
        registerPrivateActions(VertexLabelBuilder.class);
        registerPrivateActions(EdgeLabelBuilder.class);
        registerPrivateActions(IndexLabelBuilder.class);

        registerPrivateActions(TaskManager.class);
        registerPrivateActions(StandardTaskScheduler.class);
        registerPrivateActions(VortexTask.class);
        registerPrivateActions(TaskCallable.class);
        registerPrivateActions(SysTaskCallable.class);

        registerPrivateActions(StandardAuthManager.class);
        registerPrivateActions(SchemaDefine.class);
        registerPrivateActions(EntityManager.class);
        registerPrivateActions(RelationshipManager.class);

        // Don't shield them because need to access by auth RPC
        //registerPrivateActions(VortexUser.class);
        //registerPrivateActions(RolePermission.class);
        //registerPrivateActions(VortexResource.class);

        registerPrivateActions(CacheManager.class);
        registerPrivateActions(LockManager.class);
        registerPrivateActions(LicenseVerifier.class);
        registerPrivateActions(ServerReporter.class);
        registerPrivateActions(JsonSerializer.class);
        registerPrivateActions(VortexVertexStepStrategy.class);
        registerPrivateActions(VortexStepStrategy.class);
        registerPrivateActions(VortexCountStepStrategy.class);
    }

    private static void registerPrivateActions(Class<?> clazz) {
        while (clazz != Object.class) {
            List<String> fields = new ArrayList<>();
            for (Field field : clazz.getDeclaredFields()) {
                if (!Modifier.isPublic(field.getModifiers())) {
                    fields.add(field.getName());
                }
            }
            List<String> methods = new ArrayList<>();
            for (Method method : clazz.getDeclaredMethods()) {
                if (!Modifier.isPublic(method.getModifiers()) ||
                    PROTECT_METHODS.contains(method.getName())) {
                    methods.add(method.getName());
                }
            }
            registerClass(clazz, fields, methods);
            clazz = clazz.getSuperclass();
        }
    }

    private static boolean registerClass(Class<?> clazz,
                                         List<String> fields,
                                         List<String> methods) {
        if (clazz.getName().startsWith("java") ||
            fields.isEmpty() && methods.isEmpty()) {
            return false;
        }
        final String[] array = new String[fields.size()];
        try {
            Reflection.registerFieldsToFilter(clazz, fields.toArray(array));
            Reflection.registerMethodsToFilter(clazz, methods.toArray(array));
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("Filter already registered: class")) {
                return false;
            }
            throw e;
        }

        String code;
        code = String.format("Reflection.registerFieldsToFilter(%s.class, \"%s\");",
                             clazz.getCanonicalName(), String.join("\", \"", fields));
        if (!fields.isEmpty()) {
            System.out.println(code);
        }

        code = String.format("Reflection.registerMethodsToFilter(%s.class, \"%s\");",
                             clazz.getCanonicalName(), String.join("\", \"", methods));
        if (!methods.isEmpty()) {
            System.out.println(code);
        }

        return true;
    }

    private static Class<?> c(String clazz) {
        try {
            return Class.forName(clazz);
        } catch (ClassNotFoundException e) {
            throw new VortexException(e.getMessage(), e);
        }
    }
}
