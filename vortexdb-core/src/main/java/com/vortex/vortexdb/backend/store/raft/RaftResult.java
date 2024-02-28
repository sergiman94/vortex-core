
package com.vortex.vortexdb.backend.store.raft;

import com.alipay.sofa.jraft.Status;
import com.vortex.common.util.E;

import java.util.function.Supplier;

public final class RaftResult<T> {

    private final Status status;
    private final Supplier<T> callback;
    private final Throwable exception;

    public RaftResult(Status status, Supplier<T> callback) {
        this(status, callback, null);
    }

    public RaftResult(Status status, Throwable exception) {
        this(status, () -> null, exception);
    }

    public RaftResult(Status status, Supplier<T> callback,
                      Throwable exception) {
        E.checkNotNull(status, "status");
        E.checkNotNull(callback, "callback");
        this.status = status;
        this.callback = callback;
        this.exception = exception;
    }

    public Status status() {
        return this.status;
    }

    public Supplier<T> callback() {
        return this.callback;
    }

    public Throwable exception() {
        return this.exception;
    }
}
