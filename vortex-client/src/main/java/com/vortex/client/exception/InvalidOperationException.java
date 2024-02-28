package com.vortex.client.exception;

import com.vortex.common.rest.ClientException;

public class InvalidOperationException extends ClientException {

    private static final long serialVersionUID = -7618213317796656644L;

    public InvalidOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidOperationException(String message, Object... args) {
        super(message, args);
    }
}
