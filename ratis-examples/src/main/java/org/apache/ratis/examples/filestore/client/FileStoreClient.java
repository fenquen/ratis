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

    private final RaftClient raftClient;

    public FileStoreClient(RaftClient raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public void close() throws IOException {
        raftClient.close();
    }

    private ByteString sendReqSync(Message request) throws IOException {
        return sendReqSync(request, raftClient.io()::send);
    }

    private ByteString sendReqSync(Message request,
                                   CheckedFunction<Message, RaftClientReply, IOException> sendReqFunc) throws IOException {
        RaftClientReply reply = sendReqFunc.apply(request);
        StateMachineException sme = reply.getStateMachineException();
        if (sme != null) {
            throw new IOException("Failed to send request " + request, sme);
        }
        Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
        return reply.getMessage().getContent();
    }


    private CompletableFuture<ByteString> sendReqAsync(Message request,
                                                       Function<Message, CompletableFuture<RaftClientReply>> sendReqFunc) {
        return sendReqFunc.apply(request).thenApply(raftClientReply -> {
            StateMachineException stateMachineException = raftClientReply.getStateMachineException();
            if (stateMachineException != null) {
                throw new CompletionException("Failed to send request " + request, stateMachineException);
            }

            Preconditions.assertTrue(raftClientReply.isSuccess(), () -> "Failed " + request + ", reply=" + raftClientReply);
            return raftClientReply.getMessage().getContent();
        });
    }

    public ByteString readSync(String path, long offset, long length) throws IOException {
        ByteString reply = readImpl(this::sendReqReadOnlySync, path, offset, length);
        return ReadReplyProto.parseFrom(reply).getData();
    }

    private ByteString sendReqReadOnlySync(Message request) throws IOException {
        return sendReqSync(request, raftClient.io()::sendReadOnly);
    }

    public CompletableFuture<ByteString> readAsync(String path, long offset, long length) {
        return readImpl(this::sendReadOnlyAsync, path, offset, length)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(() -> ReadReplyProto.parseFrom(reply).getData()));
    }

    private CompletableFuture<ByteString> sendReadOnlyAsync(Message request) {
        return sendReqAsync(request, raftClient.async()::sendReadOnly);
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT readImpl(CheckedFunction<Message, OUTPUT, THROWABLE> sendReqFunc,
                                                                         String path,
                                                                         long offset,
                                                                         long length) throws THROWABLE {
        ReadRequestProto read = ReadRequestProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path))
                .setOffset(offset)
                .setLength(length)
                .build();

        return sendReqFunc.apply(Message.valueOf(read));
    }

    private CompletableFuture<ByteString> sendWatchAsync(Message request) {
        return sendReqAsync(request, raftClient.async()::sendReadOnlyUnordered);
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

    public DataStreamOutput getStreamOutput(String path,
                                            long dataSize,
                                            RoutingTable routingTable) {
        StreamWriteRequestProto streamWriteRequestProto =
                StreamWriteRequestProto.newBuilder()
                        .setPath(ProtoUtils.toByteString(path))
                        .setLength(dataSize)
                        .build();

        FileStoreRequestProto fileStoreRequestProto = FileStoreRequestProto.newBuilder().setStream(streamWriteRequestProto).build();

        return raftClient.getDataStreamApi().stream(fileStoreRequestProto.toByteString().asReadOnlyByteBuffer(), routingTable);
    }

    // ----------------------------------------write------------------------------------------------------------//
    public long writeSync(String path, long offset, boolean close, ByteBuffer buffer, boolean sync) throws IOException {
        int chunkSize = FileStoreCommon.getChunkSize(buffer.remaining());
        buffer.limit(chunkSize);
        ByteString reply = writeImpl(this::sendReqSync, path, offset, close, buffer, sync);
        return WriteReplyProto.parseFrom(reply).getLength();
    }

    public CompletableFuture<Long> writeAsync(String path,
                                              long offset,
                                              boolean close,
                                              ByteBuffer buffer,
                                              boolean sync) {
        return writeImpl(this::writeAsync, path, offset, close, buffer, sync)
                .thenApply(byteString -> JavaUtils.supplyAndWrapAsCompletionException(() -> WriteReplyProto.parseFrom(byteString).getLength()));
    }

    private CompletableFuture<ByteString> writeAsync(Message request) {
        return sendReqAsync(request, raftClient.async()::send);
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

    public String delete(String path) throws IOException {
        ByteString reply = deleteImpl(this::sendReqSync, path);
        return DeleteReplyProto.parseFrom(reply).getResolvedPath().toStringUtf8();
    }

    public CompletableFuture<String> deleteAsync(String path) {
        return deleteImpl(this::writeAsync, path).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> DeleteReplyProto.parseFrom(reply).getResolvedPath().toStringUtf8()));
    }

    private <OUTPUT, THROWABLE extends Throwable> OUTPUT deleteImpl(CheckedFunction<Message, OUTPUT, THROWABLE> sendFunction,
                                                                    String path) throws THROWABLE {
        DeleteRequestProto.Builder deleteRequestProtoBuilder = DeleteRequestProto.newBuilder().setPath(ProtoUtils.toByteString(path));
        FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setDelete(deleteRequestProtoBuilder).build();
        return sendFunction.apply(Message.valueOf(request));
    }
}
