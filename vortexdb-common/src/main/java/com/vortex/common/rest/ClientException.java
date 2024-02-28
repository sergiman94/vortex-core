

package com.vortex.common.rest;

public class ClientException extends RuntimeException {

    private static final long serialVersionUID = 814572040103754705L;

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(String message, Object... args) {
        super(String.format(message, args));
    }

    public ClientException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }
}
