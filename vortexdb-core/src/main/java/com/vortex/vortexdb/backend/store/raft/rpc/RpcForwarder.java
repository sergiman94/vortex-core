
package com.vortex.vortexdb.backend.store.raft.rpc;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.util.Endpoint;
import com.vortex.vortexdb.backend.BackendException;
import com.vortex.vortexdb.backend.store.raft.RaftClosure;
import com.vortex.vortexdb.backend.store.raft.RaftNode;
import com.vortex.vortexdb.backend.store.raft.RaftStoreClosure;
import com.vortex.vortexdb.backend.store.raft.StoreCommand;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;

import static com.vortex.vortexdb.backend.store.raft.RaftSharedContext.WAIT_RPC_TIMEOUT;

public class RpcForwarder {

    private static final Logger LOG = Log.logger(RpcForwarder.class);

    private final PeerId nodeId;
    private final RaftClientService rpcClient;

    public RpcForwarder(RaftNode node) {
        this.nodeId = node.node().getNodeId().getPeerId();
        this.rpcClient = ((NodeImpl) node.node()).getRpcService();
        E.checkNotNull(this.rpcClient, "rpc client");
    }

    public void forwardToLeader(PeerId leaderId, StoreCommand command,
                                RaftStoreClosure closure) {
        E.checkNotNull(leaderId, "leader id");
        E.checkState(!leaderId.equals(this.nodeId),
                     "Invalid state: current node is the leader, there is " +
                     "no need to forward the request");
        LOG.debug("The node {} forward request to leader {}",
                  this.nodeId, leaderId);

        StoreCommandRequest.Builder builder = StoreCommandRequest.newBuilder();
        builder.setType(command.type());
        builder.setAction(command.action());
        builder.setData(ZeroByteStringHelper.wrap(command.data()));
        StoreCommandRequest request = builder.build();

        RpcResponseClosure<StoreCommandResponse> responseClosure;
        responseClosure = new RpcResponseClosure<StoreCommandResponse>() {
            @Override
            public void setResponse(StoreCommandResponse response) {
                if (response.getStatus()) {
                    LOG.debug("StoreCommandResponse status ok");
                    closure.complete(Status.OK(), () -> null);
                } else {
                    LOG.debug("StoreCommandResponse status error");
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    BackendException e = new BackendException(
                                         "Current node isn't leader, leader " +
                                         "is [%s], failed to forward request " +
                                         "to leader: %s",
                                         leaderId, response.getMessage());
                    closure.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                closure.run(status);
            }
        };
        this.waitRpc(leaderId.getEndpoint(), request, responseClosure);
    }

    public <T extends Message> RaftClosure<T> forwardToLeader(PeerId leaderId,
                                                              Message request) {
        E.checkNotNull(leaderId, "leader id");
        E.checkState(!leaderId.equals(this.nodeId),
                     "Invalid state: current node is the leader, there is " +
                     "no need to forward the request");
        LOG.debug("The node '{}' forward request to leader '{}'",
                  this.nodeId, leaderId);

        RaftClosure<T> future = new RaftClosure<>();
        RpcResponseClosure<T> responseClosure = new RpcResponseClosure<T>() {
            @Override
            public void setResponse(T response) {
                FieldDescriptor fd = response.getDescriptorForType()
                                             .findFieldByName("common");
                Object object = response.getField(fd);
                E.checkState(object instanceof CommonResponse,
                             "The common field must be instance of " +
                             "CommonResponse, actual is '%s'",
                             object != null ? object.getClass() : null);
                CommonResponse commonResponse = (CommonResponse) object;
                if (commonResponse.getStatus()) {
                    future.complete(Status.OK(), () -> response);
                } else {
                    Status status = new Status(RaftError.UNKNOWN,
                                               "fowared request failed");
                    BackendException e = new BackendException(
                                         "Current node isn't leader, leader " +
                                         "is [%s], failed to forward request " +
                                         "to leader: %s",
                                         leaderId, commonResponse.getMessage());
                    future.failure(status, e);
                }
            }

            @Override
            public void run(Status status) {
                future.run(status);
            }
        };
        this.waitRpc(leaderId.getEndpoint(), request, responseClosure);
        return future;
    }

    private <T extends Message> void waitRpc(Endpoint endpoint, Message request,
                                             RpcResponseClosure<T> done) {
        E.checkNotNull(endpoint, "leader endpoint");
        try {
            this.rpcClient.invokeWithDone(endpoint, request, done,
                                          WAIT_RPC_TIMEOUT).get();
        } catch (InterruptedException e) {
            throw new BackendException("Invoke rpc request was interrupted, " +
                                       "please try again later", e);
        } catch (ExecutionException e) {
            throw new BackendException("Failed to invoke rpc request", e);
        }
    }
}
