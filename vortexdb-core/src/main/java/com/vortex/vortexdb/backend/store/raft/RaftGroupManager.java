
package com.vortex.vortexdb.backend.store.raft;

import java.util.List;

public interface RaftGroupManager {

    public String group();

    public List<String> listPeers();

    public String getLeader();

    public String transferLeaderTo(String endpoint);

    public String setLeader(String endpoint);

    public String addPeer(String endpoint);

    public String removePeer(String endpoint);
}
