
package com.vortex.vortexdb.exception;

import com.vortex.common.util.E;

import java.util.Map;

public class VortexGremlinException extends RuntimeException {

    private static final long serialVersionUID = -8712375282196157058L;

    private final int statusCode;
    private final Map<String, Object> response;

    public VortexGremlinException(int statusCode, Map<String, Object> response) {
        E.checkNotNull(response, "response");
        this.statusCode = statusCode;
        this.response = response;
    }

    public int statusCode() {
        return this.statusCode;
    }

    public Map<String, Object> response() {
        return this.response;
    }
}
