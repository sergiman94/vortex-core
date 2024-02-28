
package com.vortex.vortexdb.exception;

import com.vortex.vortexdb.VortexException;

public class NotAllowException extends VortexException {

    private static final long serialVersionUID = -1407924451828873200L;

    public NotAllowException(String message) {
        super(message);
    }

    public NotAllowException(String message, Object... args) {
        super(message, args);
    }
}
