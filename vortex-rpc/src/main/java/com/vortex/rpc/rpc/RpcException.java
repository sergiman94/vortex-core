
package com.vortex.rpc.rpc;

public class RpcException extends RuntimeException {

    private static final long serialVersionUID = -6067652498161184537L;

    public RpcException(String message) {
        super(message);
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }

    public RpcException(String message, Object... args) {
        super(String.format(message, args));
    }

    public RpcException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }
}
