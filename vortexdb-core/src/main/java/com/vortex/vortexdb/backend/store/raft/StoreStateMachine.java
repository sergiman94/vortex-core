
package com.vortex.vortexdb.backend.store.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.serializer.BytesBuffer;
import com.vortex.vortexdb.backend.store.BackendMutation;
import com.vortex.vortexdb.backend.store.BackendStore;
import com.vortex.vortexdb.backend.store.raft.RaftBackendStore.IncrCounter;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType;
import com.vortex.common.util.E;
import com.vortex.vortexdb.util.LZ4Util;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public final class StoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = Log.logger(StoreStateMachine.class);

    private final RaftSharedContext context;
    private final StoreSnapshotFile snapshotFile;

    public StoreStateMachine(RaftSharedContext context) {
        this.context = context;
        this.snapshotFile = new StoreSnapshotFile(context.stores());
    }

    private BackendStore store(StoreType type) {
        return this.context.originStore(type);
    }

    private RaftNode node() {
        return this.context.node();
    }

    @Override
    public void onApply(Iterator iter) {
        LOG.debug("Node role: {}", this.node().selfIsLeader() ?
                                   "leader" : "follower");
        List<Future<?>> futures = new ArrayList<>();
        try {
            // Apply all the logs
            while (iter.hasNext()) {
                RaftStoreClosure closure = (RaftStoreClosure) iter.done();
                if (closure != null) {
                    futures.add(this.onApplyLeader(closure));
                } else {
                    futures.add(this.onApplyFollower(iter.getData()));
                }
                iter.next();
            }

            // Wait for all tasks finished
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Throwable e) {
            String title = "StateMachine occured critical error";
            LOG.error("{}", title, e);
            Status status = new Status(RaftError.ESTATEMACHINE,
                                       "%s: %s", title, e.getMessage());
            // Will cause current node inactive
            // TODO: rollback to correct index
            iter.setErrorAndRollback(1L, status);
        }
    }

    private Future<?> onApplyLeader(RaftStoreClosure closure) {
        // Leader just take the command out from the closure
        StoreCommand command = closure.command();
        BytesBuffer buffer = BytesBuffer.wrap(command.data());
        // The first two bytes are StoreType and StoreAction
        StoreType type = StoreType.valueOf(buffer.read());
        StoreAction action = StoreAction.valueOf(buffer.read());
        boolean forwarded = command.forwarded();
        // Let the producer thread to handle it, and wait for it
        CompletableFuture<Object> future = new CompletableFuture<>();
        closure.complete(Status.OK(), () -> {
            Object result;
            try {
                result = this.applyCommand(type, action, buffer, forwarded);
            } catch (Throwable e) {
                future.completeExceptionally(e);
                throw e;
            }
            future.complete(result);
            return result;
        });
        return future;
    }

    private Future<?> onApplyFollower(ByteBuffer data) {
        // Follower need to read mutation data
        byte[] bytes = data.array();
        // Let the backend thread do it directly
        return this.context.backendExecutor().submit(() -> {
            BytesBuffer buffer = LZ4Util.decompress(bytes,
                                 RaftSharedContext.BLOCK_SIZE);
            buffer.forReadWritten();
            StoreType type = StoreType.valueOf(buffer.read());
            StoreAction action = StoreAction.valueOf(buffer.read());
            try {
                return this.applyCommand(type, action, buffer, false);
            } catch (Throwable e) {
                String title = "Failed to execute backend command";
                LOG.error("{}: {}", title, action, e);
                throw new BackendException(title, e);
            }
        });
    }

    private Object applyCommand(StoreType type, StoreAction action,
                                BytesBuffer buffer, boolean forwarded) {
        E.checkState(type != StoreType.ALL,
                     "Can't apply command for all store at one time");
        BackendStore store = this.store(type);
        switch (action) {
            case CLEAR:
                boolean clearSpace = buffer.read() > 0;
                store.clear(clearSpace);
                this.context.clearCache();
                break;
            case TRUNCATE:
                store.truncate();
                this.context.clearCache();
                break;
            case SNAPSHOT:
                assert store == null;
                this.node().snapshot();
                break;
            case BEGIN_TX:
                store.beginTx();
                break;
            case COMMIT_TX:
                List<BackendMutation> mutations = StoreSerializer.readMutations(
                                                  buffer);
                // RaftBackendStore doesn't write raft log for beginTx
                store.beginTx();
                for (BackendMutation mutation : mutations) {
                    store.mutate(mutation);
                    this.context.updateCacheIfNeeded(mutation, forwarded);
                }
                store.commitTx();
                break;
            case ROLLBACK_TX:
                store.rollbackTx();
                break;
            case INCR_COUNTER:
                // Do increase counter
                IncrCounter counter = StoreSerializer.readIncrCounter(buffer);
                store.increaseCounter(counter.type(), counter.increment());
                break;
            default:
                throw new IllegalArgumentException("Invalid action " + action);
        }
        return null;
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        LOG.info("The node {} start snapshot saving", this.node().nodeId());
        this.snapshotFile.save(writer, done, this.context.snapshotExecutor());
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        if (this.node() != null && this.node().selfIsLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        /*
         * Snapshot load occured in RaftNode constructor, specifically at step
         * `this.node = this.initRaftNode()`, at this time the NodeImpl is null
         * in RaftNode so we can't call `this.node().nodeId()`
         */
        LOG.info("The node {} start snapshot loading", this.context.endpoint());
        return this.snapshotFile.load(reader);
    }

    @Override
    public void onLeaderStart(long term) {
        LOG.info("The node {} become to leader", this.context.endpoint());
        this.node().onLeaderInfoChange(this.context.endpoint(), true);
        super.onLeaderStart(term);
    }

    @Override
    public void onLeaderStop(Status status) {
        LOG.info("The node {} abdicated from leader", this.node().nodeId());
        this.node().onLeaderInfoChange(null, false);
        super.onLeaderStop(status);
    }

    @Override
    public void onStartFollowing(LeaderChangeContext ctx) {
        LOG.info("The node {} become to follower", this.node().nodeId());
        this.node().onLeaderInfoChange(ctx.getLeaderId(), false);
        super.onStartFollowing(ctx);
    }

    @Override
    public void onStopFollowing(LeaderChangeContext ctx) {
        LOG.info("The node {} abdicated from follower", this.node().nodeId());
        this.node().onLeaderInfoChange(null, false);
        super.onStopFollowing(ctx);
    }

    @Override
    public void onConfigurationCommitted(Configuration conf) {
        super.onConfigurationCommitted(conf);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e.getMessage(), e);
    }
}
