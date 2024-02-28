package com.vortex.vortexdb;

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;

import java.io.InterruptedIOException;

public class VortexException extends RuntimeException {

    private static final long serialVersionUID = 8711375282196157058L;

    public VortexException(String message) {super(message); }
    public VortexException(String message, Throwable cause) { super(message, cause); }
    public VortexException(String message, Object...args) {super(String.format(message, args));}
    public VortexException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }

    public Throwable rootCause(){return rootCause(this);}

    public static Throwable rootCause(Throwable e) {
        Throwable cause = e;
        while(cause.getCause() != null) {
            cause = cause.getCause();
        }

        return cause;
    }

    public static boolean isInterrupted(Throwable e) {
        Throwable rootCause = VortexException.rootCause(e);
        return rootCause instanceof InterruptedException ||
                rootCause instanceof TraversalInterruptedException ||
                rootCause instanceof InterruptedIOException;
    }
}
