
package com.vortex.vortexdb.exception;

import com.vortex.vortexdb.VortexException;

public class ConnectionException extends VortexException {

    private static final long serialVersionUID = -2224809756208190785L;

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionException(String message, Object... args) {
        super(message, args);
    }

    public ConnectionException(String message, Throwable cause,
                               Object... args) {
        super(message, cause, args);
    }
}
