
package com.vortex.vortexdb.backend.store.raft;

import com.vortex.vortexdb.VortexException;

public class RaftException extends VortexException {

    private static final long serialVersionUID = 594903805213423817L;

    public RaftException(String message) {
        super(message);
    }

    public RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public RaftException(String message, Object... args) {
        super(message, args);
    }

    public RaftException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

    public RaftException(Throwable cause) {
        this("Exception in raft", cause);
    }

    public static final void check(boolean expression,
                                   String message, Object... args)
                                   throws RaftException {
        if (!expression) {
            throw new RaftException(message, args);
        }
    }
}
