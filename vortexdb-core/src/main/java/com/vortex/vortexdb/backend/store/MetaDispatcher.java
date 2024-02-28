
package com.vortex.vortexdb.backend.store;

import com.vortex.vortexdb.exception.NotSupportException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetaDispatcher<Session extends BackendSession> {

    protected final Map<String, MetaHandler<Session>> metaHandlers;

    public MetaDispatcher() {
        this.metaHandlers = new ConcurrentHashMap<>();
    }

    public void registerMetaHandler(String meta, MetaHandler<Session> handler) {
        this.metaHandlers.put(meta, handler);
    }

    @SuppressWarnings("unchecked")
    public <R> R dispatchMetaHandler(Session session,
                                     String meta, Object[] args) {
        if (!this.metaHandlers.containsKey(meta)) {
            throw new NotSupportException("metadata '%s'", meta);
        }
        return (R) this.metaHandlers.get(meta).handle(session, meta, args);
    }
}
