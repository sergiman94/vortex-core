
package com.vortex.vortexdb.backend.store;

public interface MetaHandler<Session extends com.vortex.vortexdb.backend.store.BackendSession> {

    public Object handle(Session session, String meta, Object... args);
}
