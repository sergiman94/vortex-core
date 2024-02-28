
package com.vortex.api.version;

import com.vortex.common.util.VersionUtil;
import com.vortex.common.util.VersionUtil.Version;
import com.vortex.vortexdb.version.CoreVersion;

public final class ApiVersion {

    /**
     * API Version change log
     *
     * version 0.2:
     * [0.2] vortex-527: First add the version to the vortex module
     * [0.3] vortex-525: Add versions check of components and api
     * [0.4] vortex-162: Add schema builder to seperate client and
     *       inner interface.
     * [0.5] vortex-498: Support three kind of id strategy
     *
     * version 0.3:
     *
     * [0.6] vortex-614: Add update api of VL/EL to support append and
     *       eliminate action
     * [0.7] vortex-245: Add nullable-props for vertex label and edge label
     * [0.8] vortex-396: Continue to improve variables implementation
     * [0.9] vortex-894: Add vertex/edge update api to add property and
     *       remove property
     * [0.10] vortex-919: Add condition query for vertex/edge list API
     *
     * version 0.4:
     * [0.11] vortex-938: Remove useless indexnames field in VL/EL API
     * [0.12] vortex-589: Add schema id for all schema element
     * [0.13] vortex-956: Support customize string/number id strategy
     *
     * version 0.5:
     * [0.14] vortex-1085: Add enable_label_index to VL/EL
     * [0.15] vortex-1105: Support paging for large amounts of records
     * [0.16] vortex-944: Support rest shortest path, k-out, k-neighbor
     * [0.17] vortex-944: Support rest shortest path, k-out, k-neighbor
     * [0.18] vortex-81: Change argument "checkVertex" to "check_vertex"
     *
     * version 0.6:
     * [0.19] vortex-1195: Support eliminate userdata on schema
     * [0.20] vortex-1210: Add paths api to find paths between two nodes
     * [0.21] vortex-1197: Expose scan api for vortex-spark
     * [0.22] vortex-1162: Support authentication and permission control
     * [0.23] vortex-1176: Support degree and capacity for traverse api
     * [0.24] vortex-1261: Add param offset for vertex/edge list API
     * [0.25] vortex-1272: Support set/clear restore status of graph
     * [0.26] vortex-1273: Add some monitoring counters to integrate with
     *        gremlin's monitoring framework
     * [0.27] vortex-889: Use asynchronous mechanism to do schema deletion
     *
     * version 0.8:
     * [0.28] Issue-153: Add task-cancel API
     * [0.29] Issue-39: Add rays and rings RESTful API
     * [0.30] Issue-32: Change index create API to return indexLabel and task id
     * [0.31] Issue-182: Support restore graph in restoring and merging mode
     *
     * version 0.9:
     * [0.32] Issue-250: Keep depth and degree consistent for traverser api
     * [0.33] Issue-305: Implement customized paths and crosspoints RESTful API
     * [0.34] Issue-307: Let VertexAPI use simplified property serializer
     * [0.35] Issue-287: Support pagination when do index query
     * [0.36] Issue-360: Support paging for scan api
     * [0.37] Issue-391: Add skip_super_node for shortest path
     * [0.38] Issue-274: Add personalrank and neighborrank RESTful API
     *
     * version 0.10:
     * [0.39] Issue-522: Add profile RESTful API
     * [0.40] Issue-523: Add source_in_ring args for rings RESTful API
     * [0.41] Issue-493: Support batch updating properties by multiple strategy
     * [0.42] Issue-176: Let gremlin error response consistent with RESTful's
     * [0.43] Issue-270 & 398: support shard-index and vertex + sortkey prefix,
     *        and split range to rangeInt, rangeFloat, rangeLong and rangeDouble
     * [0.44] Issue-633: Support unique index
     * [0.45] Issue-673: Add 'OVERRIDE' update strategy
     * [0.46] Issue-618 & 694: Support UUID id type
     * [0.47] Issue-691: Support aggregate property
     * [0.48] Issue-686: Support get schema by names
     *
     * version 0.11:
     * [0.49] Issue-670: Support fusiform similarity API
     * [0.50] Issue-746: Support userdata for index label
     * [0.51] Issue-929: Support 5 TP RESTful API
     * [0.52] Issue-781: Support range query for rest api like P.gt(18)
     * [0.53] Issue-985: Add grant permission API
     * [0.54] Issue-295: Support ttl for vertex and edge
     * [0.55] Issue-994: Support results count for kneighbor/kout/rings
     * [0.56] Issue-800: Show schema status in schema API
     * [0.57] Issue-1105: Allow not rebuild index when create index label
     * [0.58] Issue-1173: Supports customized kout/kneighbor,
     *        multi-node-shortest-path, jaccard-similar and template-paths
     * [0.59] Issue-1333: Support graph read mode for olap property
     * [0.60] Issue-1392: Support create and resume snapshot
     * [0.61] Issue-1433: Unify naming of degree for oltp algorithms
     * [0.62] Issue-1378: Add compact api for rocksdb/cassandra/hbase backend
     * [0.63] Issue-1500: Add user-login RESTful API
     * [0.64] Issue-1504: Add auth-project RESTful API
     * [0.65] Issue-1506: Support olap property key
     * [0.66] Issue-1567: Support get schema RESTful API
     * [0.67] Issue-1065: Support dynamically add/remove graph
     */

    // The second parameter of Version.of() is for IDE running without JAR
    public static final Version VERSION = Version.of(ApiVersion.class, "0.67");

    public static final void check() {
        // Check version of vortex-core. Firstly do check from version 0.3
        // todo: make sure the right version (uncomment this will give an error - fix it)
        //VersionUtil.check(CoreVersion.VERSION, "0.12", "0.13", CoreVersion.NAME);
    }
}
