
package com.vortex.vortexdb.backend.store.raft.rpc;

import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.vortex.vortexdb.backend.store.raft.RaftGroupManager;
import com.vortex.vortexdb.backend.store.raft.RaftSharedContext;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse;
import com.vortex.common.util.Log;
import com.google.protobuf.Message;
import org.slf4j.Logger;

public class SetLeaderProcessor
       extends RpcRequestProcessor<SetLeaderRequest> {

    private static final Logger LOG = Log.logger(SetLeaderProcessor.class);

    private final RaftSharedContext context;

    public SetLeaderProcessor(RaftSharedContext context) {
        super(null, null);
        this.context = context;
    }

    @Override
    public Message processRequest(SetLeaderRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing SetLeaderRequest {}", request.getClass());
        RaftGroupManager nodeManager = this.context.raftNodeManager(
                                       RaftSharedContext.DEFAULT_GROUP);
        try {
            nodeManager.setLeader(request.getEndpoint());
            CommonResponse common = CommonResponse.newBuilder()
                                                  .setStatus(true)
                                                  .build();
            return SetLeaderResponse.newBuilder().setCommon(common).build();
        } catch (Throwable e) {
            CommonResponse common = CommonResponse.newBuilder()
                                                  .setStatus(false)
                                                  .setMessage(e.toString())
                                                  .build();
            return SetLeaderResponse.newBuilder().setCommon(common).build();
        }
    }

    @Override
    public String interest() {
        return SetLeaderRequest.class.getName();
    }
}
