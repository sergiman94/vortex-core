
package com.vortex.vortexdb.backend.store.raft.rpc;

import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.vortex.vortexdb.backend.store.raft.RaftNode;
import com.vortex.vortexdb.backend.store.raft.RaftSharedContext;
import com.vortex.vortexdb.backend.store.raft.RaftStoreClosure;
import com.vortex.vortexdb.backend.store.raft.StoreCommand;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType;
import com.vortex.common.util.Log;
import com.google.protobuf.Message;
import org.slf4j.Logger;

public class StoreCommandProcessor
       extends RpcRequestProcessor<StoreCommandRequest> {

    private static final Logger LOG = Log.logger(
                                      StoreCommandProcessor.class);

    private final RaftSharedContext context;

    public StoreCommandProcessor(RaftSharedContext context) {
        super(null, null);
        this.context = context;
    }

    @Override
    public Message processRequest(StoreCommandRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing StoreCommandRequest: {}", request.getAction());
        RaftNode node = this.context.node();
        try {
            StoreCommand command = this.parseStoreCommand(request);
            RaftStoreClosure closure = new RaftStoreClosure(command);
            node.submitAndWait(command, closure);
            // TODO: return the submitAndWait() result to rpc client
            return StoreCommandResponse.newBuilder().setStatus(true).build();
        } catch (Throwable e) {
            LOG.warn("Failed to process StoreCommandRequest: {}",
                     request.getAction(), e);
            StoreCommandResponse.Builder builder = StoreCommandResponse
                                                   .newBuilder()
                                                   .setStatus(false);
            if (e.getMessage() != null) {
                builder.setMessage(e.getMessage());
            }
            return builder.build();
        }
    }

    @Override
    public String interest() {
        return StoreCommandRequest.class.getName();
    }

    private StoreCommand parseStoreCommand(StoreCommandRequest request) {
        StoreType type = request.getType();
        StoreAction action = request.getAction();
        byte[] data = request.getData().toByteArray();
        return new StoreCommand(type, action, data, true);
    }
}
