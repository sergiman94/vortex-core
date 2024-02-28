
package com.vortex.vortexdb.backend.store;

import com.vortex.vortexdb.backend.store.BackendStore.TxState;

/**
 * interface Session for backend store
 */
public interface BackendSession {

    public void open();
    public void close();

    public boolean opened();
    public boolean closed();

    public Object commit();

    public void rollback();

    public boolean hasChanges();

    public int attach();
    public int detach();

    public long created();
    public long updated();
    public void update();

    public default void reconnectIfNeeded() {
        // pass
    }

    public default void reset() {
        // pass
    }

    public abstract class AbstractBackendSession implements BackendSession {

        protected boolean opened;
        private int refs;
        private TxState txState;
        private final long created;
        private long updated;

        public AbstractBackendSession() {
            this.opened = true;
            this.refs = 1;
            this.txState = TxState.CLEAN;
            this.created = System.currentTimeMillis();
            this.updated = this.created;
        }

        @Override
        public long created() {
            return this.created;
        }

        @Override
        public long updated() {
            return this.updated;
        }

        @Override
        public void update() {
            this.updated = System.currentTimeMillis();
        }

        @Override
        public boolean opened() {
            return this.opened;
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        @Override
        public int attach() {
            return ++this.refs;
        }

        @Override
        public int detach() {
            return --this.refs;
        }

        public boolean closeable() {
            return this.refs <= 0;
        }

        public TxState txState() {
            return this.txState;
        }

        public void txState(TxState state) {
            this.txState = state;
        }
    }
}
