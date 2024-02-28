
package com.vortex.vortexdb.exception;

import com.vortex.vortexdb.VortexException;

public class NoIndexException extends VortexException {

    private static final long serialVersionUID = 6297062575844576832L;

    public NoIndexException(String message) {
        super(message);
    }

    public NoIndexException(String message, Object... args) {
        super(message, args);
    }
}
