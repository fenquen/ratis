package org.apache.ratis.examples.arithmetic.cli;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.common.SubCommand;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * Client to connect arithmetic example cluster.
 */
public abstract class ArithmeticClientSubCommand extends SubCommand {

    @Override
    public void run() throws Exception {
        RaftProperties raftProperties = new RaftProperties();

        RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(raftGroupId)), raftPeers);

        RaftClient.Builder raftClientBuilder = RaftClient.newBuilder().setProperties(raftProperties);
        raftClientBuilder.setRaftGroup(raftGroup);
        raftClientBuilder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));

        operation(raftClientBuilder.build());

    }

    protected abstract void operation(RaftClient raftClient) throws IOException;
}
