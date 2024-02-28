
package com.vortex.vortexdb.backend.store;

import com.vortex.vortexdb.exception.ConnectionException;
import com.vortex.vortexdb.type.VortexType;

public abstract class AbstractBackendStore<Session extends BackendSession>
                implements BackendStore {

    private final MetaDispatcher<Session> dispatcher;

    public AbstractBackendStore() {
        this.dispatcher = new MetaDispatcher<>();
    }

    protected MetaDispatcher<Session> metaDispatcher() {
        return this.dispatcher;
    }

    public void registerMetaHandler(String name, MetaHandler<Session> handler) {
        this.dispatcher.registerMetaHandler(name, handler);
    }

    // Get metadata by key
    @Override
    public <R> R metadata(VortexType type, String meta, Object[] args) {
        Session session = this.session(type);
        MetaDispatcher<Session> dispatcher = null;
        if (type == null) {
            dispatcher = this.metaDispatcher();
        } else {
            BackendTable<Session, ?> table = this.table(type);
            dispatcher = table.metaDispatcher();
        }
        return dispatcher.dispatchMetaHandler(session, meta, args);
    }

    protected void checkOpened() throws ConnectionException {
        if (!this.opened()) {
            throw new ConnectionException(
                      "The '%s' store of %s has not been opened",
                      this.database(), this.provider().type());
        }
    }

    @Override
    public String toString() {
        return String.format("%s/%s", this.database(), this.store());
    }

    protected abstract BackendTable<Session, ?> table(VortexType type);

    // NOTE: Need to support passing null
    protected abstract Session session(VortexType type);
}
