package com.vortex.vortexdb.backend;

public interface Transaction {

    public void commit() throws BackendException;
    public void commitIfGtSize(int size) throws BackendException;
    public void rollback() throws BackendException;
    public boolean autoCommit();
    public void close();
}
