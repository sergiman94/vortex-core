package com.vortex.client.exception;

import com.vortex.common.rest.ClientException;

public class NotSupportException extends ClientException {

    private static final long serialVersionUID = -8711375282196157056L;

    private static final String PREFIX = "Not support ";

    public NotSupportException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotSupportException(String message, Object... args) {
        super(PREFIX + message, args);
    }
}
