package org.apache.ratis.examples.filestore.server.fileinfo;

import org.apache.ratis.examples.filestore.server.FileStoreDataChannel;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.*;
import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ratis.util.function.CheckedSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

public class UnderConstruction extends FileInfo {
    private FileStoreDataChannel out;

    /**
     * The size written to a local file.
     */
    private volatile long writeSize;

    /**
     * The size committed to client.
     */
    private volatile long committedSize;

    /**
     * A queue to make sure that the writes are in order.
     */
    private final TaskQueue writeQueue = new TaskQueue("writeQueue");

    private final Map<Long, WriteInfo> index_writeInfo = new ConcurrentHashMap<>();

    private final AtomicLong lastWriteIndex = new AtomicLong(-1L);

    public UnderConstruction(Path relativePath) {
        super(relativePath);
    }

    @Override
    public UnderConstruction asUnderConstruction() {
        return this;
    }

    @Override
    public long getCommittedSize() {
        return committedSize;
    }

    @Override
    public long getWriteSize() {
        return writeSize;
    }

    public CompletableFuture<Integer> create(CheckedFunction<Path, Path, IOException> resolver,
                                             ByteString data,
                                             boolean close,
                                             boolean sync,
                                             ExecutorService executorService,
                                             RaftPeerId raftPeerId,
                                             long index) {

        CheckedSupplier<Integer, IOException> writeTask = LogUtils.newCheckedSupplier(
                LOG,
                () -> {
                    if (out == null) {
                        out = new FileStoreDataChannel(resolver.apply(getRelativePath()));
                    }

                    return write(0L, data, close, sync);
                },
                () -> "create(" + getRelativePath() + ", " + close + ") @" + raftPeerId + ":" + index);

        return submitWriteTask(writeTask, executorService, raftPeerId, index);
    }

    public CompletableFuture<Integer> append(long offset,
                                             ByteString data,
                                             boolean close,
                                             boolean sync,
                                             ExecutorService executor,
                                             RaftPeerId raftPeerId,
                                             long index) {
        Supplier<String> name = () -> "write(" + getRelativePath() + ", " + offset + ", " + close + ") @" + raftPeerId + ":" + index;

        CheckedSupplier<Integer, IOException> writeTask = LogUtils.newCheckedSupplier(LOG, () -> write(offset, data, close, sync), name);

        return submitWriteTask(writeTask, executor, raftPeerId, index);
    }

    private int write(long offset,
                      ByteString data,
                      boolean close,
                      boolean sync) throws IOException {
        // If leader finish write data with offset = 4096 and writeSize become 8192,
        // and 2 follower has not written the data with offset = 4096,
        // then start a leader election. So client will retry send the data with offset = 4096,
        // then offset < writeSize in leader.
        if (offset < writeSize) {
            return data.size();
        }

        if (offset != writeSize) {
            throw new IOException("offset: " + offset + " != writeSize:" + writeSize + ", path=" + getRelativePath());
        }

        if (out == null) {
            throw new IOException("File output is not initialized, path=" + getRelativePath());
        }

        synchronized (out) {
            int n = 0;

            if (data != null) {
                ByteBuffer buffer = data.asReadOnlyByteBuffer();
                try {
                    while (buffer.remaining() > 0) {
                        n += out.write(buffer);
                    }
                } finally {
                    writeSize += n;
                }
            }

            if (sync) {
                out.force(false);
            }

            if (close) {
                out.close();
            }

            return n;
        }
    }

    private CompletableFuture<Integer> submitWriteTask(CheckedSupplier<Integer, IOException> writeTask,
                                                       ExecutorService executorService,
                                                       RaftPeerId id,
                                                       long index) {
        CompletableFuture<Integer> f = writeQueue.submit(writeTask, executorService, e -> new IOException("Failed " + writeTask, e));
        WriteInfo writeInfo = new WriteInfo(f, lastWriteIndex.getAndSet(index));
        CollectionUtils.putNew(index, writeInfo, index_writeInfo, () -> id + ":writeInfos");
        return f;
    }


    public CompletableFuture<Integer> submitCommitTask(long offset,
                                                       int size,
                                                       Function<UnderConstruction, ReadOnly> closeFunction,
                                                       ExecutorService executorService,
                                                       RaftPeerId raftPeerId,
                                                       long index) {
        boolean close = (closeFunction != null);

        Supplier<String> name = () -> "commit(" + getRelativePath() + ", " + offset + ", " + size + ", close? " + close + ") @" + raftPeerId + ":" + index;

        WriteInfo writeInfo = index_writeInfo.get(index);
        if (writeInfo == null) {
            return JavaUtils.completeExceptionally(new IOException(name.get() + " is already committed."));
        }

        CheckedSupplier<Integer, IOException> commitTask = LogUtils.newCheckedSupplier(
                LOG,
                () -> {
                    if (offset != committedSize) {
                        throw new IOException("Offset/size mismatched: offset = " + offset + " != committedSize = " + committedSize + ", path=" + getRelativePath());
                    }

                    if (committedSize + size > writeSize) {
                        throw new IOException("Offset/size mismatched: committed (=" + committedSize + ") + size (=" + size + ") > writeSize = " + writeSize);
                    }

                    committedSize += size;

                    if (close) {
                        ReadOnly ignored = closeFunction.apply(this);
                        index_writeInfo.remove(index);
                    }

                    writeInfo.commitFuture.complete(size);

                    return size;
                }, name);

        // Remove previous info, if there is any.
        WriteInfo previousWriteInfo = index_writeInfo.remove(writeInfo.previousIndex);
        CompletableFuture<Integer> previousCommitFuture = previousWriteInfo != null ? previousWriteInfo.commitFuture : CompletableFuture.completedFuture(0);

        // Commit after both current write and previous commit completed.
        return writeInfo.writeFuture.thenCombineAsync(previousCommitFuture, (wSize, previousCommitSize) -> {
            Preconditions.assertTrue(size == wSize);

            try {
                return commitTask.get();
            } catch (IOException e) {
                throw new CompletionException("Failed " + commitTask, e);
            }
        }, executorService);
    }
}
