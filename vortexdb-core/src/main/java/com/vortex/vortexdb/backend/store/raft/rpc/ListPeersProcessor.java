
package com.vortex.vortexdb.backend.store.raft.rpc;

import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.vortex.vortexdb.backend.store.raft.RaftGroupManager;
import com.vortex.vortexdb.backend.store.raft.RaftSharedContext;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest;
import com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse;
import com.vortex.common.util.Log;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.slf4j.Logger;

public class ListPeersProcessor
       extends RpcRequestProcessor<ListPeersRequest> {

    private static final Logger LOG = Log.logger(ListPeersProcessor.class);

    private final RaftSharedContext context;

    public ListPeersProcessor(RaftSharedContext context) {
        super(null, null);
        this.context = context;
    }

    @Override
    public Message processRequest(ListPeersRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing ListPeersRequest {}", request.getClass());
        RaftGroupManager nodeManager = this.context.raftNodeManager(
                                       RaftSharedContext.DEFAULT_GROUP);
        try {
            CommonResponse common = CommonResponse.newBuilder()
                                                  .setStatus(true)
                                                  .build();
            return ListPeersResponse.newBuilder()
                                    .setCommon(common)
                                    .addAllEndpoints(nodeManager.listPeers())
                                    .build();
        } catch (Throwable e) {
            CommonResponse common = CommonResponse.newBuilder()
                                                  .setStatus(false)
                                                  .setMessage(e.toString())
                                                  .build();
            return ListPeersResponse.newBuilder()
                                    .setCommon(common)
                                    .addAllEndpoints(ImmutableList.of())
                                    .build();
        }
    }

    @Override
    public String interest() {
        return ListPeersRequest.class.getName();
    }
}
