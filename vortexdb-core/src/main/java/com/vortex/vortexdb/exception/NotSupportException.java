
package com.vortex.vortexdb.exception;

import com.vortex.vortexdb.VortexException;

public class NotSupportException extends VortexException {

    private static final long serialVersionUID = -2914329541122906234L;
    private static final String PREFIX = "Not support ";

    public NotSupportException(String message) {
        super(PREFIX + message);
    }

    public NotSupportException(String message, Object... args) {
        super(PREFIX + message, args);
    }
}
