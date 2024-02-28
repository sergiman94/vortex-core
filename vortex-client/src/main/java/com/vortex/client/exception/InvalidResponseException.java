package com.vortex.client.exception;

import com.vortex.common.rest.ClientException;

public class InvalidResponseException extends ClientException {

    private static final long serialVersionUID = -6837901607110262081L;

    public InvalidResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidResponseException(String message, Object... args) {
        super(message, args);
    }

    public static InvalidResponseException expectField(String expectField,
                                                       Object parentField) {
        return new InvalidResponseException(
                   "Invalid response, expect '%s' in '%s'",
                   expectField, parentField);
    }
}
