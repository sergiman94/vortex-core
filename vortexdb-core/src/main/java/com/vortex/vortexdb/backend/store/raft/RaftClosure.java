
package com.vortex.vortexdb.backend.store.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class RaftClosure<T> implements Closure {

    private static final Logger LOG = Log.logger(RaftStoreClosure.class);

    private final CompletableFuture<RaftResult<T>> future;

    public RaftClosure() {
        this.future = new CompletableFuture<>();
    }

    public T waitFinished() throws Throwable {
        RaftResult<T> result = this.get();
        if (result.status().isOk()) {
            return result.callback().get();
        } else {
            throw result.exception();
        }
    }

    public Status status() {
        return this.get().status();
    }

    private RaftResult<T> get() {
        try {
            return this.future.get(RaftSharedContext.WAIT_RAFTLOG_TIMEOUT,
                                   TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new BackendException("ExecutionException", e);
        } catch (InterruptedException e) {
            throw new BackendException("InterruptedException", e);
        } catch (TimeoutException e) {
            throw new BackendException("Wait closure timeout");
        }
    }

    public void complete(Status status, Supplier<T> callback) {
        this.future.complete(new RaftResult<>(status, callback));
    }

    public void failure(Status status, Throwable exception) {
        this.future.complete(new RaftResult<>(status, exception));
    }

    @Override
    public void run(Status status) {
        if (status.isOk()) {
            this.complete(status, () -> null);
        } else {
            LOG.error("Failed to apply command: {}", status);
            String msg = "Failed to apply command in raft node with error: " +
                         status.getErrorMsg();
            this.failure(status, new BackendException(msg));
        }
    }
}
