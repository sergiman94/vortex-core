
package com.vortex.vortexdb.backend.store.raft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse;
import com.vortex.vortexdb.backend.store.raft.rpc.RpcForwarder;
import com.vortex.common.util.E;
import com.google.protobuf.Message;

import java.util.List;
import java.util.stream.Collectors;

public class RaftGroupManagerImpl implements RaftGroupManager {

    private final String group;
    private final RaftNode raftNode;
    private final RpcForwarder rpcForwarder;

    public RaftGroupManagerImpl(RaftSharedContext context) {
        this.group = context.group();
        this.raftNode = context.node();
        this.rpcForwarder = context.rpcForwarder();
    }

    @Override
    public String group() {
        return this.group;
    }

    @Override
    public List<String> listPeers() {
        if (this.raftNode.selfIsLeader()) {
            List<PeerId> peerIds = this.raftNode.node().listPeers();
            return peerIds.stream().map(PeerId::toString)
                          .collect(Collectors.toList());
        }
        // If current node is not leader, forward request to leader
        ListPeersRequest request = ListPeersRequest.getDefaultInstance();
        try {
            RaftClosure<ListPeersResponse> future;
            future = this.forwardToLeader(request);
            ListPeersResponse response = future.waitFinished();
            return response.getEndpointsList();
        } catch (Throwable e) {
            throw new BackendException("Failed to list peers", e);
        }
    }

    @Override
    public String getLeader() {
        PeerId leaderId = this.raftNode.leaderId();
        E.checkState(leaderId != null,
                     "There is no leader for raft group '%s'", this.group);
        return leaderId.toString();
    }

    @Override
    public String transferLeaderTo(String endpoint) {
        PeerId peerId = PeerId.parsePeer(endpoint);
        Status status = this.raftNode.node().transferLeadershipTo(peerId);
        if (!status.isOk()) {
            throw new BackendException(
                      "Failed to transafer leader to '%s', raft error: %s",
                      endpoint, status.getErrorMsg());
        }
        return peerId.toString();
    }

    @Override
    public String setLeader(String endpoint) {
        PeerId newLeaderId = PeerId.parsePeer(endpoint);
        Node node = this.raftNode.node();
        // If expected endpoint has already been raft leader
        if (node.getLeaderId().equals(newLeaderId)) {
            return newLeaderId.toString();
        }
        if (this.raftNode.selfIsLeader()) {
            // If current node is the leader, transfer directly
            this.transferLeaderTo(endpoint);
        } else {
            // If current node is not leader, forward request to leader
            SetLeaderRequest request = SetLeaderRequest.newBuilder()
                                                       .setEndpoint(endpoint)
                                                       .build();
            try {
                RaftClosure<SetLeaderResponse> future;
                future = this.forwardToLeader(request);
                future.waitFinished();
            } catch (Throwable e) {
                throw new BackendException("Failed to set leader to '%s'",
                                           e, endpoint);
            }
        }
        return newLeaderId.toString();
    }

    @Override
    public String addPeer(String endpoint) {
        E.checkArgument(this.raftNode.selfIsLeader(),
                        "Operation add_peer can only be executed on leader");
        PeerId peerId = PeerId.parsePeer(endpoint);
        RaftClosure<?> future = new RaftClosure<>();
        try {
            this.raftNode.node().addPeer(peerId, future);
            future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to add peer '%s'", e, endpoint);
        }
        return peerId.toString();
    }

    @Override
    public String removePeer(String endpoint) {
        E.checkArgument(this.raftNode.selfIsLeader(),
                        "Operation add_peer can only be executed on leader");
        PeerId peerId = PeerId.parsePeer(endpoint);
        RaftClosure<?> future = new RaftClosure<>();
        try {
            this.raftNode.node().removePeer(peerId, future);
            future.waitFinished();
        } catch (Throwable e) {
            throw new BackendException("Failed to remove peer '%s'",
                                       e, endpoint);
        }
        return peerId.toString();
    }

    private <T extends Message> RaftClosure<T> forwardToLeader(Message request) {
        PeerId leaderId = this.raftNode.leaderId();
        return this.rpcForwarder.forwardToLeader(leaderId, request);
    }
}
