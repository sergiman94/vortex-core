
package com.vortex.vortexdb.backend.store.raft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.core.Replicator.ReplicatorStateListener;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.util.LZ4Util;
import com.vortex.common.util.Log;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class RaftNode {

    private static final Logger LOG = Log.logger(RaftNode.class);

    private final RaftSharedContext context;
    private final Node node;
    private final StoreStateMachine stateMachine;
    private final AtomicReference<LeaderInfo> leaderInfo;
    private final AtomicBoolean started;
    private final AtomicInteger busyCounter;

    public RaftNode(RaftSharedContext context) {
        this.context = context;
        this.stateMachine = new StoreStateMachine(context);
        try {
            this.node = this.initRaftNode();
            LOG.info("Start raft node: {}", this);
        } catch (IOException e) {
            throw new BackendException("Failed to init raft node", e);
        }
        this.node.addReplicatorStateListener(new RaftStateListener());
        this.leaderInfo = new AtomicReference<>(LeaderInfo.NO_LEADER);
        this.started = new AtomicBoolean(false);
        this.busyCounter = new AtomicInteger();
    }

    public RaftSharedContext context() {
        return this.context;
    }

    public Node node() {
        assert this.node != null;
        return this.node;
    }

    public PeerId nodeId() {
        return this.node.getNodeId().getPeerId();
    }

    public PeerId leaderId() {
        return this.leaderInfo.get().leaderId;
    }

    public boolean selfIsLeader() {
        return this.leaderInfo.get().selfIsLeader;
    }

    public void onLeaderInfoChange(PeerId leaderId, boolean selfIsLeader) {
        leaderId = leaderId != null ? leaderId.copy() : null;
        this.leaderInfo.set(new LeaderInfo(leaderId, selfIsLeader));
    }

    public void shutdown() {
        LOG.info("Shutdown raft node: {}", this);
        this.node.shutdown();
    }

    public void snapshot() {
        if (!this.context.useSnapshot()) {
            return;
        }
        RaftClosure<?> future = new RaftClosure<>();
        try {
            this.node().snapshot(future);
            future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to create snapshot", e);
        }
    }

    private Node initRaftNode() throws IOException {
        NodeOptions nodeOptions = this.context.nodeOptions();
        nodeOptions.setFsm(this.stateMachine);
        // TODO: When support sharding, groupId needs to be bound to shard Id
        String groupId = this.context.group();
        PeerId endpoint = this.context.endpoint();
        /*
         * Start raft node with shared rpc server:
         * return new RaftGroupService(groupId, endpoint, nodeOptions,
         *                             this.context.rpcServer(), true)
         *        .start(false)
         */
        return RaftServiceFactory.createAndInitRaftNode(groupId, endpoint,
                                                        nodeOptions);
    }

    private void submitCommand(StoreCommand command, RaftStoreClosure closure) {
        // Wait leader elected
        LeaderInfo leaderInfo = this.waitLeaderElected(
                                RaftSharedContext.NO_TIMEOUT);
        if (!leaderInfo.selfIsLeader) {
            this.context.rpcForwarder().forwardToLeader(leaderInfo.leaderId,
                                                        command, closure);
            return;
        }
        // Sleep a while when raft node is busy
        this.waitIfBusy();

        Task task = new Task();
        task.setDone(closure);
        // compress return BytesBuffer
        ByteBuffer buffer = LZ4Util.compress(command.data(),
                                             RaftSharedContext.BLOCK_SIZE)
                                   .forReadWritten()
                                   .asByteBuffer();
        LOG.debug("The bytes size of command(compressed) {} is {}",
                  command.action(), buffer.limit());
        task.setData(buffer);
        LOG.debug("submit to raft node {}", this.node);
        this.node.apply(task);
    }

    public Object submitAndWait(StoreCommand command, RaftStoreClosure future) {
        this.submitCommand(command, future);
        try {
            /*
             * Here will wait future complete, actually the follower has waited
             * in forwardToLeader, written like this to simplify the code
             */
            return future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to wait store command %s",
                                       e, command);
        }
    }

    protected LeaderInfo waitLeaderElected(int timeout) {
        String group = this.context.group();
        LeaderInfo leaderInfo = this.leaderInfo.get();
        if (leaderInfo.leaderId != null) {
            return leaderInfo;
        }
        long beginTime = System.currentTimeMillis();
        while (leaderInfo.leaderId == null) {
            try {
                Thread.sleep(RaftSharedContext.POLL_INTERVAL);
            } catch (InterruptedException e) {
                LOG.info("Waiting for raft group '{}' election is " +
                         "interrupted: {}", group, e);
            }
            long consumedTime = System.currentTimeMillis() - beginTime;
            if (timeout > 0 && consumedTime >= timeout) {
                throw new BackendException(
                          "Waiting for raft group '%s' election timeout(%sms)",
                          group, consumedTime);
            }
            LOG.warn("Waiting for raft group '{}' election cost {}s",
                     group, consumedTime / 1000.0);
            leaderInfo = this.leaderInfo.get();
            assert leaderInfo != null;
        }
        return leaderInfo;
    }

    protected void waitStarted(int timeout) {
        String group = this.context.group();
        ReadIndexClosure readIndexClosure = new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                RaftNode.this.started.set(status.isOk());
            }
        };
        long beginTime = System.currentTimeMillis();
        while (true) {
            this.node.readIndex(BytesUtil.EMPTY_BYTES, readIndexClosure);
            if (this.started.get()) {
                break;
            }
            try {
                Thread.sleep(RaftSharedContext.POLL_INTERVAL);
            } catch (InterruptedException e) {
                LOG.info("Waiting for heartbeat is interrupted: {}", e);
            }
            long consumedTime = System.currentTimeMillis() - beginTime;
            if (timeout > 0 && consumedTime >= timeout) {
                throw new BackendException(
                          "Waiting for raft group '%s' heartbeat timeout(%sms)",
                          group, consumedTime);
            }
            LOG.warn("Waiting for raft group '{}' heartbeat cost {}s",
                     group, consumedTime / 1000.0);
        }
    }

    private void waitIfBusy() {
        int counter = this.busyCounter.get();
        if (counter <= 0) {
            return;
        }
        // It may lead many thread sleep, but this is exactly what I want
        long time = counter * RaftSharedContext.BUSY_SLEEP_FACTOR;
        LOG.info("The node {} will try to sleep {} ms", this.node, time);
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // Throw busy exception if the request is timeout
            throw new BackendException("The raft backend store is busy", e);
        } finally {
            if (this.busyCounter.get() > 0) {
                synchronized (this) {
                    if (this.busyCounter.get() > 0) {
                        counter = this.busyCounter.decrementAndGet();
                        LOG.info("Decrease busy counter: [{}]", counter);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return String.format("[%s-%s]", this.context.group(), this.nodeId());
    }

    protected final class RaftStateListener implements ReplicatorStateListener {

        private volatile long lastPrintTime;

        public RaftStateListener() {
            this.lastPrintTime = 0L;
        }

        @Override
        public void onCreated(PeerId peer) {
            LOG.info("The node {} replicator has created", peer);
        }

        @Override
        public void onDestroyed(PeerId peer) {
            LOG.warn("Replicator '{}' is ready to go offline", peer);
        }

        @Override
        public void onError(PeerId peer, Status status) {
            long now = System.currentTimeMillis();
            long interval = now - this.lastPrintTime;
            if (interval >= RaftSharedContext.LOG_WARN_INTERVAL) {
                LOG.warn("Replicator meet error: {}", status);
                this.lastPrintTime = now;
            }
            if (this.isWriteBufferOverflow(status)) {
                // Increment busy counter
                int count = RaftNode.this.busyCounter.incrementAndGet();
                LOG.info("Increase busy counter due to overflow: [{}]", count);
            }
        }

        // NOTE: Jraft itself doesn't have this callback, it's added by us
        public void onBusy(PeerId peer, Status status) {
            /*
             * If follower is busy then increase busy counter,
             * it will lead to submit thread wait more time
             */
            int count = RaftNode.this.busyCounter.incrementAndGet();
            LOG.info("Increase busy counter: [{}]", count);
        }

        private boolean isWriteBufferOverflow(Status status) {
            String expectMsg = "maybe write overflow";
            return RaftError.EINTERNAL == status.getRaftError() &&
                   status.getErrorMsg() != null &&
                   status.getErrorMsg().contains(expectMsg);
        }

        /**
         * Maybe useful in the future
         */
        @SuppressWarnings("unused")
        private boolean isRpcTimeout(Status status) {
            String expectMsg = "Invoke timeout";
            return RaftError.EINTERNAL == status.getRaftError() &&
                   status.getErrorMsg() != null &&
                   status.getErrorMsg().contains(expectMsg);
        }
    }

    /**
     * Jraft Node.getLeaderId() and Node.isLeader() is not always consistent,
     * We define this class to manage leader info by ourselves
     */
    private static class LeaderInfo {

        private static final LeaderInfo NO_LEADER = new LeaderInfo(null, false);

        private final PeerId leaderId;
        private final boolean selfIsLeader;

        public LeaderInfo(PeerId leaderId, boolean selfIsLeader) {
            this.leaderId = leaderId;
            this.selfIsLeader = selfIsLeader;
        }
    }
}
