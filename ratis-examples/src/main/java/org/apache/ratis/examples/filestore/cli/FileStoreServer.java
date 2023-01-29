package org.apache.ratis.examples.filestore.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.examples.common.SubCommand;
import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.examples.filestore.FileStoreStateMachine;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metrics.JVMMetrics;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Class to start a ratis filestore example server.
 */
@Parameters(commandDescription = "Start an filestore server")
public class FileStoreServer extends SubCommand {

    @Parameter(names = {"--id", "-i"}, description = "Raft id of this server", required = true)
    private String id;

    @Parameter(names = {"--storage", "-s"}, description = "Storage dir, eg. --storage dir1 --storage dir2", required = true)
    private List<File> storageDir = new ArrayList<>();

    @Parameter(names = {"--writeThreadNum"}, description = "Number of write thread")
    private int writeThreadNum = 20;

    @Parameter(names = {"--readThreadNum"}, description = "Number of read thread")
    private int readThreadNum = 20;

    @Parameter(names = {"--commitThreadNum"}, description = "Number of commit thread")
    private int commitThreadNum = 3;

    @Parameter(names = {"--deleteThreadNum"}, description = "Number of delete thread")
    private int deleteThreadNum = 3;

    @Override
    public void run() throws Exception {
        JVMMetrics.initJvmMetrics(TimeDuration.valueOf(10, TimeUnit.SECONDS));

        RaftProperties raftProperties = new RaftProperties();

        // Avoid leader change affect the performance
        RaftServerConfigKeys.Rpc.setTimeoutMin(raftProperties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(raftProperties, TimeDuration.valueOf(3, TimeUnit.SECONDS));

        RaftPeer raftPeer = getRaftPeer(RaftPeerId.valueOf(id));

        // 设置raftProperties各个属性
        {
            int port = NetUtils.createSocketAddr(raftPeer.getAddress()).getPort();
            GrpcConfigKeys.Server.setPort(raftProperties, port);

            Optional.ofNullable(raftPeer.getClientAddress()).ifPresent(address ->
                    GrpcConfigKeys.Client.setPort(raftProperties, NetUtils.createSocketAddr(address).getPort()));
            Optional.ofNullable(raftPeer.getAdminAddress()).ifPresent(address ->
                    GrpcConfigKeys.Admin.setPort(raftProperties, NetUtils.createSocketAddr(address).getPort()));

            String dataStreamAddress = raftPeer.getDataStreamAddress();
            if (dataStreamAddress != null) {
                int dataStreamPort = NetUtils.createSocketAddr(dataStreamAddress).getPort();
                NettyConfigKeys.DataStream.setPort(raftProperties, dataStreamPort);
                RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);
            }

            RaftServerConfigKeys.setStorageDir(raftProperties, storageDir);
            RaftServerConfigKeys.Write.setElementLimit(raftProperties, 40960);
            RaftServerConfigKeys.Write.setByteLimit(raftProperties, SizeInBytes.valueOf("1000MB"));
            ConfUtils.setFiles(raftProperties::setFiles, FileStoreCommon.STATEMACHINE_DIR_KEY, storageDir);
            RaftServerConfigKeys.DataStream.setAsyncRequestThreadPoolSize(raftProperties, writeThreadNum);
            RaftServerConfigKeys.DataStream.setAsyncWriteThreadPoolSize(raftProperties, writeThreadNum);

            ConfUtils.setInt(raftProperties::setInt, FileStoreCommon.STATEMACHINE_WRITE_THREAD_NUM, writeThreadNum);
            ConfUtils.setInt(raftProperties::setInt, FileStoreCommon.STATEMACHINE_READ_THREAD_NUM, readThreadNum);
            ConfUtils.setInt(raftProperties::setInt, FileStoreCommon.STATEMACHINE_COMMIT_THREAD_NUM, commitThreadNum);
            ConfUtils.setInt(raftProperties::setInt, FileStoreCommon.STATEMACHINE_DELETE_THREAD_NUM, deleteThreadNum);
        }


        RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(raftGroupId)), raftPeers);
        RaftServer raftServer = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(id))
                .setStateMachine(new FileStoreStateMachine(raftProperties)).setProperties(raftProperties)
                .setGroup(raftGroup)
                .build();

        raftServer.start();

        while (raftServer.getLifeCycleState() != LifeCycle.State.CLOSED) {
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
