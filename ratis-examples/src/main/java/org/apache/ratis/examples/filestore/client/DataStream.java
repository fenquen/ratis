package org.apache.ratis.examples.filestore.client;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

/**
 * Subcommand to generate load in file store data stream state machine.
 */
@Parameters(commandDescription = "Load Generator for FileStore DataStream")
public class DataStream extends FileStoreClientSubCommand {
    @Parameter(names = {"--type"}, description = "[DirectByteBuffer, MappedByteBuffer, NettyFileRegion]", required = true)
    private String dataStreamType = TransferType.NettyFileRegion.name();

    @Parameter(names = {"--syncSize"},
            description = "Sync every syncSize, syncSize % bufferSize should be zero,-1 means on sync",
            required = true)
    private int syncSize = -1;

    int getSyncSize() {
        return syncSize;
    }

    private boolean checkParam() {
        if (syncSize != -1 && syncSize % bufferSizeInBytes != 0) {
            System.err.println("Error: syncSize % bufferSize should be zero");
            return false;
        }

        return true;
    }

    @Override
    protected void operation(List<FileStoreClient> fileStoreClientList) throws Exception {
        if (!checkParam()) {
            stop(fileStoreClientList);
        }

        final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
        List<String> paths = generateLocalFiles(executor);
        FileStoreCommon.dropCache();
        System.out.println("Starting DataStream write now ");

        RoutingTable routingTable = getRoutingTable(Arrays.asList(raftPeers), getPrimary());
        long startTime = System.currentTimeMillis();

        long totalWrittenBytes = waitStreamFinish(streamWrite(paths, fileStoreClientList, routingTable, executor));

        System.out.println("Total files written: " + getNumFiles());
        System.out.println("Each files size: " + getFileSizeInByte());
        System.out.println("Total data written: " + totalWrittenBytes + " bytes");
        System.out.println("Total time taken: " + (System.currentTimeMillis() - startTime) + " millis");

        stop(fileStoreClientList);
    }

    private Map<String, CompletableFuture<List<CompletableFuture<DataStreamReply>>>> streamWrite(List<String> paths,
                                                                                                 List<FileStoreClient> clients,
                                                                                                 RoutingTable routingTable,
                                                                                                 ExecutorService executor) {
        Map<String, CompletableFuture<List<CompletableFuture<DataStreamReply>>>> path_future = new HashMap<>();

        int clientIndex = 0;
        for (String path : paths) {
            CompletableFuture<List<CompletableFuture<DataStreamReply>>> future = new CompletableFuture<>();
            FileStoreClient client = clients.get(clientIndex++ % clients.size());

            CompletableFuture.supplyAsync(() -> {

                Preconditions.assertTrue(new File(path).length() == fileSizeInByte, "unexpected size");

                TransferType transferType = TransferType.valueOf(dataStreamType);
                Transfer transfer = transferType.constructor.apply(path, this);

                try {
                    future.complete(transfer.transfer(client, routingTable));
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
                return future;
            }, executor);

            path_future.put(path, future);
        }

        return path_future;
    }

    private long waitStreamFinish(Map<String, CompletableFuture<List<CompletableFuture<DataStreamReply>>>> fileMap)
            throws ExecutionException, InterruptedException {
        long totalBytes = 0;
        for (CompletableFuture<List<CompletableFuture<DataStreamReply>>> futures : fileMap.values()) {
            long writtenLen = 0;
            for (CompletableFuture<DataStreamReply> future : futures.get()) {
                writtenLen += future.join().getBytesWritten();
            }

            if (writtenLen != getFileSizeInByte()) {
                System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInByte());
            }

            totalBytes += writtenLen;
        }
        return totalBytes;
    }

    enum TransferType {
        DirectByteBuffer(DirectByteBufferType::new),
        MappedByteBuffer(MappedByteBufferType::new),
        NettyFileRegion(NettyFileRegionType::new);

        private final BiFunction<String, DataStream, Transfer> constructor;

        TransferType(BiFunction<String, DataStream, Transfer> constructor) {
            this.constructor = constructor;
        }
    }

    private abstract static class Transfer {
        private final String path;
        private final File file;
        private final long fileSize;
        protected final int bufferSize;
        private final long syncSize;
        private long syncPosition = 0;

        Transfer(String path, DataStream cli) {
            this.path = path;
            this.file = new File(path);
            this.fileSize = cli.getFileSizeInByte();
            this.bufferSize = cli.getBufferSizeInBytes();
            this.syncSize = cli.getSyncSize();

            final long actualSize = file.length();
            Preconditions.assertTrue(actualSize == fileSize, () -> "Unexpected file size: expected size is "
                    + fileSize + " but actual size is " + actualSize + ", path=" + path);
        }

        File getFile() {
            return file;
        }


        long getPacketSize(long offset) {
            return Math.min(bufferSize, fileSize - offset);
        }

        boolean isSync(long position) {
            if (syncSize > 0) {
                if (position >= fileSize || position - syncPosition >= syncSize) {
                    syncPosition = position;
                    return true;
                }
            }
            return false;
        }

        List<CompletableFuture<DataStreamReply>> transfer(FileStoreClient fileStoreClient,
                                                          RoutingTable routingTable) throws IOException {
            if (fileSize <= 0) {
                return Collections.emptyList();
            }

            List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
            DataStreamOutput out = fileStoreClient.getStreamOutput(file.getName(), fileSize, routingTable);

            try (FileChannel inputFileChannel = new FileInputStream(file).getChannel()) {
                for (long offset = 0L; offset < fileSize; ) {
                    offset += write(inputFileChannel, out, offset, futures);
                }
            } catch (Throwable e) {
                throw new IOException("Failed to transfer " + path);
            } finally {
                futures.add(out.closeAsync());
            }

            return futures;
        }

        public abstract long write(FileChannel inputFileChannel,
                                   DataStreamOutput dataStreamOutput,
                                   long offset,
                                   List<CompletableFuture<DataStreamReply>> futures) throws IOException;

        @Override
        public String toString() {
            return JavaUtils.getClassSimpleName(getClass()) + "{" + path + ", size=" + fileSize + "}";
        }
    }

    private static class DirectByteBufferType extends Transfer {
        DirectByteBufferType(String path, DataStream cli) {
            super(path, cli);
        }

        @Override
        public long write(FileChannel inputFileChannel,
                          DataStreamOutput dataStreamOutput,
                          long offset,
                          List<CompletableFuture<DataStreamReply>> futures) throws IOException {

            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(bufferSize);

            int bytesRead = buf.writeBytes(inputFileChannel, bufferSize);
            if (bytesRead < 0) {
                throw new IllegalStateException("Failed to read " + bufferSize + " byte(s) from " + this + ". The channel has reached end-of-stream at " + offset);
            }

            if (bytesRead > 0) {
                CompletableFuture<DataStreamReply> f = isSync(offset + bytesRead) ?
                        dataStreamOutput.writeAsync(buf.nioBuffer(), StandardWriteOption.SYNC) : dataStreamOutput.writeAsync(buf.nioBuffer());
                f.thenRun(buf::release);
                futures.add(f);
            }

            return bytesRead;
        }
    }

    private static class MappedByteBufferType extends Transfer {
        MappedByteBufferType(String path, DataStream cli) {
            super(path, cli);
        }

        @Override
        public long write(FileChannel inputFileChannel,
                          DataStreamOutput dataStreamOutput,
                          long offset,
                          List<CompletableFuture<DataStreamReply>> futures) throws IOException {
            MappedByteBuffer mappedByteBuffer = inputFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, getPacketSize(offset));
            int remaining = mappedByteBuffer.remaining();
            futures.add(isSync(offset + remaining) ?
                    dataStreamOutput.writeAsync(mappedByteBuffer, StandardWriteOption.SYNC) : dataStreamOutput.writeAsync(mappedByteBuffer));
            return remaining;
        }
    }

    private static class NettyFileRegionType extends Transfer {
        NettyFileRegionType(String path, DataStream cli) {
            super(path, cli);
        }

        @Override
        public long write(FileChannel inputFileChannel, DataStreamOutput dataStreamOutput, long offset, List<CompletableFuture<DataStreamReply>> futures) {
            final long packetSize = getPacketSize(offset);
            futures.add(isSync(offset + packetSize) ?
                    dataStreamOutput.writeAsync(getFile(), offset, packetSize, StandardWriteOption.SYNC) :
                    dataStreamOutput.writeAsync(getFile(), offset, packetSize));
            return packetSize;
        }
    }
}
