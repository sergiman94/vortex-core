
package com.vortex.distr;

import com.vortex.common.event.EventHub;
import com.vortex.api.server.RestServer;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

public class VortexRestServer {

    private static final Logger LOG = Log.logger(VortexRestServer.class);

    public static RestServer start(String conf, EventHub hub) throws Exception {
        // Start RestServer
        return RestServer.start(conf, hub);
    }
}
