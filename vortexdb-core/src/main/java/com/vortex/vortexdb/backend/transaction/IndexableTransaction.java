package com.vortex.vortexdb.backend.transaction;

import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.store.BackendMutation;
import com.vortex.vortexdb.backend.store.BackendStore;

public abstract class IndexableTransaction extends AbstractTransaction {

    public IndexableTransaction(VortexParams graph, BackendStore store) {
        super(graph, store);
    }

    @Override
    public boolean hasUpdate() {
        AbstractTransaction indexTx = this.indexTransaction();
        boolean indexTxChanged = (indexTx != null && indexTx.hasUpdate());
        return indexTxChanged || super.hasUpdate();
    }

    @Override
    protected void reset() {
        super.reset();

        // It's null when called by super AbstractTransaction()
        AbstractTransaction indexTx = this.indexTransaction();
        if (indexTx != null) {
            indexTx.reset();
        }
    }

    @Override
    protected void commit2Backend() {
        BackendMutation mutation = this.prepareCommit();
        BackendMutation idxMutation = this.indexTransaction().prepareCommit();
        assert !mutation.isEmpty() || !idxMutation.isEmpty();
        // Commit graph/schema updates and index updates with graph/schema tx
        this.commitMutation2Backend(mutation, idxMutation);
    }

    @Override
    public void commitIfGtSize(int size) throws BackendException {
        int totalSize = this.mutationSize() +
                this.indexTransaction().mutationSize();
        if (totalSize >= size) {
            this.commit();
        }
    }

    @Override
    public void rollback() throws BackendException {
        try {
            super.rollback();
        } finally {
            this.indexTransaction().rollback();
        }
    }

    @Override
    public void close() {
        try {
            this.indexTransaction().close();
        } finally {
            super.close();
        }
    }

    protected abstract AbstractTransaction indexTransaction();
}
