
package com.vortex.vortexdb.exception;

import com.vortex.vortexdb.VortexException;

public class LimitExceedException extends VortexException {

    private static final long serialVersionUID = 7384276720045597709L;

    public LimitExceedException(String message) {
        super(message);
    }

    public LimitExceedException(String message, Object... args) {
        super(message, args);
    }
}
