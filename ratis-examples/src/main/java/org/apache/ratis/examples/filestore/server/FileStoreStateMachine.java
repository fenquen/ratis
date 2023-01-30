package org.apache.ratis.examples.filestore.server;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.examples.filestore.server.FileStore;
import org.apache.ratis.proto.ExamplesProtos;
import org.apache.ratis.proto.ExamplesProtos.DeleteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.DeleteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.FileStoreRequestProto;
import org.apache.ratis.proto.ExamplesProtos.ReadRequestProto;
import org.apache.ratis.proto.ExamplesProtos.StreamWriteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.WriteRequestHeaderProto;
import org.apache.ratis.proto.ExamplesProtos.WriteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class FileStoreStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();

    private final FileStore fileStore;

    public FileStoreStateMachine(RaftProperties raftProperties) {
        fileStore = new FileStore(this::getId, raftProperties);
    }

    @Override
    public void initialize(RaftServer raftServer,
                           RaftGroupId raftGroupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(raftServer, raftGroupId, raftStorage);

        simpleStateMachineStorage.init(raftStorage);

        for (Path path : fileStore.getRoots()) {
            FileUtils.createDirectories(path);
        }
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return simpleStateMachineStorage;
    }

    @Override
    public void close() {
        fileStore.close();
        setLastAppliedTermIndex(null);
    }

    /**
     * 客户端的write请求首先到该函数
     */
    @Override
    public TransactionContext startTransaction(RaftClientRequest raftClientRequest) throws IOException {
        ByteString content = raftClientRequest.getMessage().getContent();

        TransactionContext.Builder txCtxBuilder = TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(raftClientRequest);

        FileStoreRequestProto fileStoreRequestProto = FileStoreRequestProto.parseFrom(content);

        if (fileStoreRequestProto.getRequestCase() == FileStoreRequestProto.RequestCase.WRITE) {
            WriteRequestProto writeRequestProto = fileStoreRequestProto.getWrite();
            FileStoreRequestProto newProto = FileStoreRequestProto.newBuilder().setWriteHeader(writeRequestProto.getHeader()).build();
            txCtxBuilder.setLogData(newProto.toByteString()).setStateMachineData(writeRequestProto.getData());
        } else {
            txCtxBuilder.setLogData(content);
        }

        return txCtxBuilder.build();
    }

    @Override
    public CompletableFuture<Integer> write(LogEntryProto logEntryProto) {
        StateMachineLogEntryProto stateMachineLogEntry = logEntryProto.getStateMachineLogEntry();

        FileStoreRequestProto fileStoreRequestProto;
        try {
            fileStoreRequestProto = FileStoreRequestProto.parseFrom(stateMachineLogEntry.getLogData());
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally(logEntryProto.getIndex(), "Failed to parse data, entry=" + logEntryProto, e);
        }

        if (fileStoreRequestProto.getRequestCase() != FileStoreRequestProto.RequestCase.WRITEHEADER) {
            return null;
        }

        WriteRequestHeaderProto writeRequestHeaderProto = fileStoreRequestProto.getWriteHeader();

        CompletableFuture<Integer> future =
                fileStore.write(
                        logEntryProto.getIndex(),
                        writeRequestHeaderProto.getPath().toStringUtf8(),
                        writeRequestHeaderProto.getClose(),
                        writeRequestHeaderProto.getSync(),
                        writeRequestHeaderProto.getOffset(),
                        stateMachineLogEntry.getStateMachineEntry().getStateMachineData());

        // sync only if closing the file
        return writeRequestHeaderProto.getClose() ? future : null;
    }

    @Override
    public CompletableFuture<ByteString> read(LogEntryProto logEntryProto) {
        FileStoreRequestProto fileStoreRequestProto;
        try {
            ByteString data = logEntryProto.getStateMachineLogEntry().getLogData();
            fileStoreRequestProto = FileStoreRequestProto.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally(logEntryProto.getIndex(), "Failed to parse data, entry=" + logEntryProto, e);
        }

        if (fileStoreRequestProto.getRequestCase() != FileStoreRequestProto.RequestCase.WRITEHEADER) {
            return null;
        }

        WriteRequestHeaderProto writeRequestHeaderProto = fileStoreRequestProto.getWriteHeader();
        CompletableFuture<ReadReplyProto> future =
                fileStore.read(
                        writeRequestHeaderProto.getPath().toStringUtf8(),
                        writeRequestHeaderProto.getOffset(),
                        writeRequestHeaderProto.getLength(),
                        false);

        return future.thenApply(ReadReplyProto::getData);
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        ReadRequestProto readRequestProto;
        try {
            readRequestProto = ReadRequestProto.parseFrom(request.getContent());
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally("Failed to parse " + request, e);
        }

        String path = readRequestProto.getPath().toStringUtf8();
        return (readRequestProto.getIsWatch() ?
                fileStore.watch(path) :
                fileStore.read(path, readRequestProto.getOffset(), readRequestProto.getLength(), true))
                .thenApply(reply -> Message.valueOf(reply.toByteString()));
    }

    private static class LocalStream implements DataStream {
        private final DataChannel dataChannel;

        LocalStream(DataChannel dataChannel) {
            this.dataChannel = dataChannel;
        }

        @Override
        public DataChannel getDataChannel() {
            return dataChannel;
        }

        @Override
        public CompletableFuture<?> cleanUp() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    dataChannel.close();
                    return true;
                } catch (IOException e) {
                    return FileStoreCommon.completeExceptionally("Failed to close data channel", e);
                }
            });
        }
    }

    @Override
    public CompletableFuture<DataStream> stream(RaftClientRequest request) {
        final ByteString reqByteString = request.getMessage().getContent();
        final FileStoreRequestProto proto;
        try {
            proto = FileStoreRequestProto.parseFrom(reqByteString);
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally("Failed to parse stream header", e);
        }

        return fileStore.createDataChannel(proto.getStream().getPath().toStringUtf8()).thenApply(LocalStream::new);
    }

    @Override
    public CompletableFuture<?> link(DataStream stream, LogEntryProto entry) {
        LOG.info("linking {}", stream);
        return fileStore.streamLink(stream);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext transactionContext) {
        LogEntryProto logEntryProto = transactionContext.getLogEntry();

        long index = logEntryProto.getIndex();

        updateLastAppliedTermIndex(logEntryProto.getTerm(), index);

        final StateMachineLogEntryProto smLog = logEntryProto.getStateMachineLogEntry();
        final FileStoreRequestProto request;
        try {
            request = FileStoreRequestProto.parseFrom(smLog.getLogData());
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally(index, "Failed to parse logData in" + smLog, e);
        }

        switch (request.getRequestCase()) {
            case DELETE:
                return delete(index, request.getDelete());
            case WRITEHEADER:
                return writeCommit(index, request.getWriteHeader(), smLog.getStateMachineEntry().getStateMachineData().size());
            case STREAM:
                return streamCommit(request.getStream());
            case WRITE:
                // WRITE should not happen here since
                // startTransaction converts WRITE requests to WRITEHEADER requests.
            default:
                LOG.error(getId() + ": Unexpected request case " + request.getRequestCase());
                return FileStoreCommon.completeExceptionally(index, "Unexpected request case " + request.getRequestCase());
        }
    }

    private CompletableFuture<Message> delete(long index, DeleteRequestProto request) {
        String path = request.getPath().toStringUtf8();
        return fileStore.delete(index, path).thenApply(resolved ->
                Message.valueOf(DeleteReplyProto.newBuilder().setResolvedPath(FileStoreCommon.toByteString(resolved)).build().toByteString(),
                        () -> "Message:" + resolved));
    }

    private CompletableFuture<Message> writeCommit(long index, WriteRequestHeaderProto header, int size) {
        String path = header.getPath().toStringUtf8();
        return fileStore.submitCommit(index, path, header.getClose(), header.getOffset(), size)
                .thenApply(reply -> Message.valueOf(reply.toByteString()));
    }

    private CompletableFuture<Message> streamCommit(StreamWriteRequestProto stream) {
        String path = stream.getPath().toStringUtf8();
        long size = stream.getLength();
        return fileStore.streamCommit(path, size).thenApply(reply -> Message.valueOf(reply.toByteString()));
    }

}
