
package com.vortex.vortexdb.backend.store.memory;

import com.vortex.vortexdb.backend.store.BackendMetrics;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class InMemoryMetrics implements BackendMetrics {

    @Override
    public Map<String, Object> metrics() {
        return ImmutableMap.of(NODES, 1);
    }
}
