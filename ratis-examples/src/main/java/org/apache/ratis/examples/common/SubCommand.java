package org.apache.ratis.examples.common;

import com.beust.jcommander.Parameter;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Base subcommand class which includes the basic raft properties.
 */
public abstract class SubCommand {

    @Parameter(names = {"--raftGroup", "-g"}, description = "Raft group identifier")
    public String raftGroupId = "demoRaftGroup123";

    @Parameter(names = {"--peers", "-r"},
            description = "Raft peers (format: name:host:port:dataStreamPort:clientPort:adminPort, ...)",required = true)
    public String raftPeersStr;

    public RaftPeer[] raftPeers;

    public SubCommand() {
        raftPeers = Stream.of(raftPeersStr.split(",")).map(peerAddr -> {
            String[] peerAddrElementArr = peerAddr.split(":");
            if (peerAddrElementArr.length < 3) {
                throw new IllegalArgumentException("Raft peer " + peerAddr + " is not a legitimate format. (format: name:host:port:dataStreamPort:clientPort:adminPort)");
            }

            RaftPeer.Builder raftPeerBuilder = RaftPeer.newBuilder();

            raftPeerBuilder.setId(peerAddrElementArr[0])
                    .setAddress(peerAddrElementArr[1] + ":" + peerAddrElementArr[2]);

            if (peerAddrElementArr.length >= 4) {
                raftPeerBuilder.setDataStreamAddress(peerAddrElementArr[1] + ":" + peerAddrElementArr[3]);
                if (peerAddrElementArr.length >= 5) {
                    raftPeerBuilder.setClientAddress(peerAddrElementArr[1] + ":" + peerAddrElementArr[4]);
                    if (peerAddrElementArr.length >= 6) {
                        raftPeerBuilder.setAdminAddress(peerAddrElementArr[1] + ":" + peerAddrElementArr[5]);
                    }
                }
            }

            return raftPeerBuilder.build();
        }).toArray(RaftPeer[]::new);
    }

    public RaftPeer getPrimary() {
        return raftPeers[0];
    }

    public abstract void run() throws Exception;

    public RoutingTable getRoutingTable(List<RaftPeer> raftPeerList, RaftPeer primary) {
        RoutingTable.Builder builder = RoutingTable.newBuilder();
        RaftPeer previous = primary;
        for (RaftPeer peer : raftPeerList) {
            if (peer.equals(primary)) {
                continue;
            }
            builder.addSuccessor(previous.getId(), peer.getId());
            previous = peer;
        }

        return builder.build();
    }

    public RaftPeer getRaftPeer(RaftPeerId raftPeerId) {
        for (RaftPeer raftPeer : raftPeers) {
            if (raftPeerId.equals(raftPeer.getId())) {
                return raftPeer;
            }
        }

        throw new IllegalArgumentException("Raft peer id " + raftPeerId + " is not part of the raft group definitions " + raftPeersStr);
    }
}