package com.vortex.vortexdb.backend;

import com.vortex.vortexdb.VortexException;

public class BackendException extends VortexException {


    public BackendException(String message) {super(message);}

    public BackendException(String message, Throwable cause) {super(message, cause);}

    public BackendException(String message, Object... args) {super(message, args);}

    public BackendException(String message, Throwable cause, Object... args) {super(message, cause, args);}

    public BackendException(Throwable cause) {this("Exception in backend", cause);}

    public static final void check(boolean expression, String message, Object... args)
        throws BackendException {
        if(!expression)
            throw new BackendException(message, args);
    }



}
