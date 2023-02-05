package org.apache.ratis.examples.filestore.client;

import com.beust.jcommander.Parameter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.examples.common.SubCommand;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Client to connect file store example cluster.
 */
public abstract class FileStoreClientSubCommand extends SubCommand {

    /**
     * 单个的文件的大小都是1样
     */
    @Parameter(names = {"--size"},
            description = "Size of each file in bytes",
            required = true)
    protected long fileSizeInByte;

    @Parameter(names = {"--bufferSize"},
            description = "Size of buffer in bytes, should less than 4MB, i.e BUFFER_BYTE_LIMIT_DEFAULT")
    protected int bufferSizeInBytes = 1024;

    @Parameter(names = {"--numFiles"},
            description = "Number of files to be written",
            required = true)
    private int numFiles;

    @Parameter(names = {"--numClients"},
            description = "Number of clients to write",
            required = true)
    private int numClients;

    @Parameter(names = {"--storage", "-s"},
            description = "Storage dir, eg. --storage dir1 --storage dir2",
            required = true)
    private List<File> storageDirList = new ArrayList<>();

    private static final int MAX_THREADS_NUM = 1000;

    public int getNumThread() {
        return numFiles < MAX_THREADS_NUM ? numFiles : MAX_THREADS_NUM;
    }

    public long getFileSizeInByte() {
        return fileSizeInByte;
    }

    public int getBufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    public int getNumFiles() {
        return numFiles;
    }

    @Override
    public void run() throws Exception {
        int raftSegmentPreAllocatedSize = 1024 * 1024 * 1024;

        RaftProperties raftProperties = new RaftProperties();

        RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
        RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

        GrpcConfigKeys.setMessageSizeMax(raftProperties, SizeInBytes.valueOf(raftSegmentPreAllocatedSize));

        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties, SizeInBytes.valueOf(raftSegmentPreAllocatedSize));
        RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties, SizeInBytes.valueOf(raftSegmentPreAllocatedSize));
        RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties, SizeInBytes.valueOf(raftSegmentPreAllocatedSize));
        RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties, SizeInBytes.valueOf(1024 * 1024 * 1024L));
        RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

        RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties, TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
        RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);

        for (File dir : storageDirList) {
            FileUtils.createDirectories(dir);
        }

        operation(buildFileStoreClientList(raftProperties));
    }

    private List<FileStoreClient> buildFileStoreClientList(RaftProperties raftProperties) {
        List<FileStoreClient> fileStoreClientList = new ArrayList<>();
        for (int i = 0; i < numClients; i++) {
            RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(raftGroupId)), raftPeers);

            RaftClient.Builder builder = RaftClient.newBuilder();
            builder.setProperties(raftProperties);
            builder.setRaftGroup(raftGroup);
            builder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
            builder.setPrimaryDataStreamServer(getPrimary());

            fileStoreClientList.add(new FileStoreClient(builder.build()));
        }
        return fileStoreClientList;
    }

    @SuppressFBWarnings("DM_EXIT")
    protected void stop(List<FileStoreClient> fileStoreClientList) throws IOException {
        for (FileStoreClient fileStoreClient : fileStoreClientList) {
            fileStoreClient.close();
        }

        System.exit(0);
    }

    protected List<String> generateLocalFiles(ExecutorService executorService) {
        UUID uuid = UUID.randomUUID();

        List<String> generatedLocalFileAbsPathList = new ArrayList<>();
        List<CompletableFuture<Long>> futureList = new ArrayList<>();

        for (int i = 0; i < numFiles; i++) {
            String generatedLocalFileAbsPath = getLocalFileAbsPath("file-" + uuid + "-" + i);
            generatedLocalFileAbsPathList.add(generatedLocalFileAbsPath);
            futureList.add(writeFileAsync(generatedLocalFileAbsPath, executorService));
        }

        for (int i = 0; i < futureList.size(); i++) {
            long size = futureList.get(i).join();
            if (size != fileSizeInByte) {
                System.err.println("Error: path:" + generatedLocalFileAbsPathList.get(i) + " write:" + size + " mismatch expected size:" + fileSizeInByte);
            }
        }

        return generatedLocalFileAbsPathList;
    }

    private String getLocalFileAbsPath(String fileName) {
        int storageDirIndex = fileName.hashCode() % storageDirList.size();
        return new File(storageDirList.get(Math.abs(storageDirIndex)), fileName).getAbsolutePath();
    }


    private CompletableFuture<Long> writeFileAsync(String path, ExecutorService executorService) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        CompletableFuture.supplyAsync(() -> {
            try {
                future.complete(writeFile(path, fileSizeInByte, bufferSizeInBytes));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
            return future;
        }, executorService);
        return future;
    }

    private long writeFile(String path, long fileSize, long bufferSize) throws IOException {
        byte[] buffer = new byte[Math.toIntExact(bufferSize)];
        long offset = 0;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw")) {
            while (offset < fileSize) {
                long remaining = fileSize - offset;
                long chunkSize = Math.min(remaining, bufferSize);

                ThreadLocalRandom.current().nextBytes(buffer);

                randomAccessFile.write(buffer, 0, Math.toIntExact(chunkSize));

                offset += chunkSize;
            }
        }
        return offset;
    }

    protected abstract void operation(List<FileStoreClient> fileStoreClientList) throws Exception;
}
