
package com.vortex.vortexdb.backend.store;

import java.util.Map;

public interface BackendMetrics {

    public String BACKEND = "backend";

    public String NODES = "nodes";
    public String CLUSTER_ID = "cluster_id";
    public String SERVERS = "servers";
    public String SERVER_LOCAL = "local";
    public String SERVER_CLUSTER = "cluster";

    // Memory related metrics
    public String MEM_USED = "mem_used";
    public String MEM_COMMITTED = "mem_committed";
    public String MEM_MAX = "mem_max";
    public String MEM_UNIT = "mem_unit";

    // Data load related metrics
    public String DISK_USAGE = "disk_usage";
    public String DISK_UNIT = "disk_unit";

    public String READABLE = "_readable";

    public String EXCEPTION = "exception";

    public Map<String, Object> metrics();
}
