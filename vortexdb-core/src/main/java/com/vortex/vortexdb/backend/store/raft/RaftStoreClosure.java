
package com.vortex.vortexdb.backend.store.raft;

import com.vortex.common.util.E;

public class RaftStoreClosure extends RaftClosure<Object> {

    private final StoreCommand command;

    public RaftStoreClosure(StoreCommand command) {
        E.checkNotNull(command, "store command");
        this.command = command;
    }

    public StoreCommand command() {
        return this.command;
    }
}
