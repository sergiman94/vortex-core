
package com.vortex.vortexdb.backend.store;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.transaction.SchemaTransaction;
import com.vortex.vortexdb.schema.PropertyKey;
import com.vortex.vortexdb.schema.SchemaElement;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.slf4j.Logger;

import java.util.Map;

public class BackendStoreSystemInfo {

    private static final Logger LOG = Log.logger(Vortex.class);

    private static final String PK_BACKEND_INFO = Hidden.hide("backend_info");

    private final SchemaTransaction schemaTx;

    public BackendStoreSystemInfo(SchemaTransaction schemaTx) {
        this.schemaTx = schemaTx;
    }

    public synchronized void init() {
        if (this.exists()) {
            return;
        }
        // Set schema counter to reserve primitive system id
        this.schemaTx.setNextIdLowest(VortexType.SYS_SCHEMA,
                                      SchemaElement.MAX_PRIMITIVE_SYS_ID);

        Vortex graph = this.schemaTx.graph();
        E.checkState(this.info() == null,
                     "Already exists backend info of graph '%s' in backend " +
                     "'%s'", graph.name(), graph.backend());
        // Use property key to store backend version
        String backendVersion = graph.backendVersion();
        PropertyKey backendInfo = graph.schema()
                                       .propertyKey(PK_BACKEND_INFO)
                                       .userdata("version", backendVersion)
                                       .build();
        this.schemaTx.addPropertyKey(backendInfo);
    }

    private Map<String, Object> info() {
        PropertyKey pkey;
        try {
            pkey = this.schemaTx.getPropertyKey(PK_BACKEND_INFO);
        } catch (IllegalStateException e) {
            String message = String.format(
                             "Should not exist schema with same name '%s'",
                             PK_BACKEND_INFO);
            if (message.equals(e.getMessage())) {
                Vortex graph = this.schemaTx.graph();
                throw new VortexException("There exists multiple backend info " +
                                        "of graph '%s' in backend '%s'",
                                        graph.name(), graph.backend());
            }
            throw e;
        }
        return pkey != null ? pkey.userdata() : null;
    }

    public boolean exists() {
        if (!this.schemaTx.storeInitialized()) {
            return false;
        }
        return this.info() != null;
    }

    public boolean checkVersion() {
        Map<String, Object> info = this.info();
        E.checkState(info != null, "The backend version info doesn't exist");
        // Backend has been initialized
        Vortex graph = this.schemaTx.graph();
        String driverVersion = graph.backendVersion();
        String backendVersion = (String) info.get("version");
        if (!driverVersion.equals(backendVersion)) {
            LOG.error("The backend driver version '{}' is inconsistent with " +
                      "the data version '{}' of backend store for graph '{}'",
                      driverVersion, backendVersion, graph.name());
            return false;
        }
        return true;
    }
}
