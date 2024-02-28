
package com.vortex.common.rest;

public class SerializeException extends ClientException {

    private static final long serialVersionUID = -4622753445618619311L;

    public SerializeException(String message, Throwable e) {
        super(message, e);
    }

    public SerializeException(String message, Object... args) {
        super(message, args);
    }

    public SerializeException(String message, Throwable e, Object... args) {
        super(message, e, args);
    }
}
