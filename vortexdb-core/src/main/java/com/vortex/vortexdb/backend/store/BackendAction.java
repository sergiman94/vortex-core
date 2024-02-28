
package com.vortex.vortexdb.backend.store;

import com.vortex.vortexdb.type.define.Action;

public class BackendAction {

    private final Action action;
    private final com.vortex.vortexdb.backend.store.BackendEntry entry;

    public static BackendAction of(Action action, com.vortex.vortexdb.backend.store.BackendEntry entry) {
        return new BackendAction(entry, action);
    }

    public BackendAction(com.vortex.vortexdb.backend.store.BackendEntry entry, Action action) {
        this.action = action;
        this.entry = entry;
    }

    public Action action() {
        return this.action;
    }

    public com.vortex.vortexdb.backend.store.BackendEntry entry() {
        return this.entry;
    }

    @Override
    public String toString() {
        return String.format("entry: %s, action: %s", this.entry, this.action);
    }
}
