
package com.vortex.vortexdb.exception;

import com.vortex.vortexdb.VortexException;

public class NotFoundException extends VortexException {

    private static final long serialVersionUID = -5912665926327173032L;

    public NotFoundException(String message) {
        super(message);
    }

    public NotFoundException(String message, Object... args) {
        super(message, args);
    }

    public NotFoundException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }
}
