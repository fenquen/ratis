package org.apache.ratis.examples.filestore.client;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.proto.ExamplesProtos.DeleteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.DeleteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.FileStoreRequestProto;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.proto.ExamplesProtos.ReadRequestProto;
import org.apache.ratis.proto.ExamplesProtos.StreamWriteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.WriteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.WriteRequestHeaderProto;
import org.apache.ratis.proto.ExamplesProtos.WriteRequestProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * A standalone server using raft with a configurable state machine.
 */
public class FileStoreClient implements Closeable {
    public static final Logger LOG = LoggerFactory.getLogger(FileStoreClient.class);

    private final RaftClient raftClient;

    public FileStoreClient(RaftClient raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public void close() throws IOException {
        raftClient.close();
    }

    private ByteString send(Message request) throws IOException {
        return send(request, raftClient.io()::send);
    }

    private ByteString sendReadOnly(Message request) throws IOException {
        return send(request, raftClient.io()::sendReadOnly);
    }

    private ByteString send(Message request,
                            CheckedFunction<Message, RaftClientReply, IOException> sendFunction) throws IOException {
        RaftClientReply reply = sendFunction.apply(request);
        StateMachineException sme = reply.getStateMachineException();
        if (sme != null) {
            throw new IOException("Failed to send request " + request, sme);
        }
        Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
        return reply.getMessage().getContent();
    }

    private CompletableFuture<ByteString> sendAsync(Message request) {
        return sendAsync(request, raftClient.async()::send);
    }

    private CompletableFuture<ByteString> sendReadOnlyAsync(Message request) {
        return sendAsync(request, raftClient.async()::sendReadOnly);
    }

    private CompletableFuture<ByteString> sendAsync(Message request,
                                                    Function<Message, CompletableFuture<RaftClientReply>> sendFunction) {
        return sendFunction.apply(request).thenApply(raftClientReply -> {
            StateMachineException stateMachineException = raftClientReply.getStateMachineException();
            if (stateMachineException != null) {
                throw new CompletionException("Failed to send request " + request, stateMachineException);
            }

            Preconditions.assertTrue(raftClientReply.isSuccess(), () -> "Failed " + request + ", reply=" + raftClientReply);
            return raftClientReply.getMessage().getContent();
        });
    }

    public ByteString read(String path, long offset, long length) throws IOException {
        final ByteString reply = readImpl(this::sendReadOnly, path, offset, length);
        return ReadReplyProto.parseFrom(reply).getData();
    }

    public CompletableFuture<ByteString> readAsync(String path, long offset, long length) {
        return readImpl(this::sendReadOnlyAsync, path, offset, length
        ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> ReadReplyProto.parseFrom(reply).getData()));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT readImpl(CheckedFunction<Message, OUTPUT, THROWABLE> sendReadOnlyFunction,
                                                                         String path,
                                                                         long offset,
                                                                         long length) throws THROWABLE {
        ReadRequestProto read = ReadRequestProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path))
                .setOffset(offset)
                .setLength(length)
                .build();

        return sendReadOnlyFunction.apply(Message.valueOf(read));
    }

    private CompletableFuture<ByteString> sendWatchAsync(Message request) {
        return sendAsync(request, raftClient.async()::sendReadOnlyUnordered);
    }

    /**
     * Watch the path until it is created.
     */
    public CompletableFuture<ReadReplyProto> watchAsync(String path) {
        return watchImpl(this::sendWatchAsync, path)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                        () -> ReadReplyProto.parseFrom(reply)));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT watchImpl(CheckedFunction<Message, OUTPUT, THROWABLE> sendWatchFunction,
                                                                          String path) throws THROWABLE {
        ReadRequestProto watch = ReadRequestProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path))
                .setIsWatch(true)
                .build();

        return sendWatchFunction.apply(Message.valueOf(watch));
    }

    public DataStreamOutput getStreamOutput(String path, long dataSize, RoutingTable routingTable) {
        StreamWriteRequestProto header = StreamWriteRequestProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path))
                .setLength(dataSize)
                .build();
        FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setStream(header).build();
        return raftClient.getDataStreamApi().stream(request.toByteString().asReadOnlyByteBuffer(), routingTable);
    }

    public long write(String path, long offset, boolean close, ByteBuffer buffer, boolean sync) throws IOException {
        int chunkSize = FileStoreCommon.getChunkSize(buffer.remaining());
        buffer.limit(chunkSize);
        ByteString reply = writeImpl(this::send, path, offset, close, buffer, sync);
        return WriteReplyProto.parseFrom(reply).getLength();
    }

    public CompletableFuture<Long> writeAsync(String path,
                                              long offset,
                                              boolean close,
                                              ByteBuffer buffer,
                                              boolean sync) {
        return writeImpl(this::sendAsync, path, offset, close, buffer, sync)
                .thenApply(byteString -> JavaUtils.supplyAndWrapAsCompletionException(() -> WriteReplyProto.parseFrom(byteString).getLength()));
    }

    private <OUTPUT, THROWABLE extends Throwable> OUTPUT writeImpl(CheckedFunction<Message, OUTPUT, THROWABLE> sendFunction,
                                                                   String path,
                                                                   long offset,
                                                                   boolean close,
                                                                   ByteBuffer data,
                                                                   boolean sync) throws THROWABLE {
        WriteRequestHeaderProto.Builder writeReqHeaderBuilder =
                WriteRequestHeaderProto.newBuilder()
                        .setPath(ProtoUtils.toByteString(path))
                        .setOffset(offset)
                        .setLength(data.remaining())
                        .setClose(close)
                        .setSync(sync);

        WriteRequestProto.Builder writeReqBuilder =
                WriteRequestProto.newBuilder()
                        .setHeader(writeReqHeaderBuilder)
                        .setData(ByteString.copyFrom(data));

        FileStoreRequestProto fileStoreRequest = FileStoreRequestProto.newBuilder().setWrite(writeReqBuilder).build();
        return sendFunction.apply(Message.valueOf(fileStoreRequest));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT deleteImpl(CheckedFunction<Message, OUTPUT, THROWABLE> sendFunction,
                                                                           String path) throws THROWABLE {
        DeleteRequestProto.Builder delete = DeleteRequestProto.newBuilder().setPath(ProtoUtils.toByteString(path));
        FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setDelete(delete).build();
        return sendFunction.apply(Message.valueOf(request));
    }

    public String delete(String path) throws IOException {
        ByteString reply = deleteImpl(this::send, path);
        return DeleteReplyProto.parseFrom(reply).getResolvedPath().toStringUtf8();
    }

    public CompletableFuture<String> deleteAsync(String path) {
        return deleteImpl(this::sendAsync, path).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> DeleteReplyProto.parseFrom(reply).getResolvedPath().toStringUtf8()));
    }
}
