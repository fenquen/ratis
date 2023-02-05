package org.apache.ratis.examples.filestore.server;

import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.examples.filestore.server.fileinfo.FileInfo;
import org.apache.ratis.examples.filestore.server.fileinfo.ReadOnly;
import org.apache.ratis.examples.filestore.server.fileinfo.UnderConstruction;
import org.apache.ratis.examples.filestore.server.fileinfo.Watch;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.proto.ExamplesProtos.StreamWriteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.WriteReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

public class FileStore implements Closeable {
    public static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

    private final Supplier<RaftPeerId> raftPeerIdSupplier;
    private final List<Supplier<Path>> storagePathSupplierList;
    private final FileInfoMap fileInfoMap;

    private final ExecutorService writeThreadPool;
    private final ExecutorService commitThreadPool;
    private final ExecutorService readThreadPool;
    private final ExecutorService deleteThreadPool;

    public FileStore(Supplier<RaftPeerId> raftPeerIdSupplier, RaftProperties raftProperties) {
        this.raftPeerIdSupplier = raftPeerIdSupplier;
        storagePathSupplierList = new ArrayList<>();

        int writeThreadNum = ConfUtils.getInt(raftProperties::getInt, FileStoreCommon.STATEMACHINE_WRITE_THREAD_NUM, 1, LOG::info);
        writeThreadPool = Executors.newFixedThreadPool(writeThreadNum);

        int readThreadNum = ConfUtils.getInt(raftProperties::getInt, FileStoreCommon.STATEMACHINE_READ_THREAD_NUM, 1, LOG::info);
        readThreadPool = Executors.newFixedThreadPool(readThreadNum);

        int commitThreadNum = ConfUtils.getInt(raftProperties::getInt, FileStoreCommon.STATEMACHINE_COMMIT_THREAD_NUM, 1, LOG::info);
        commitThreadPool = Executors.newFixedThreadPool(commitThreadNum);

        int deleteThreadNum = ConfUtils.getInt(raftProperties::getInt, FileStoreCommon.STATEMACHINE_DELETE_THREAD_NUM, 1, LOG::info);
        deleteThreadPool = Executors.newFixedThreadPool(deleteThreadNum);

        List<File> storageDirList = ConfUtils.getFiles(raftProperties::getFiles, FileStoreCommon.STATEMACHINE_DIR_KEY, null, LOG::info);
        Objects.requireNonNull(storageDirList, FileStoreCommon.STATEMACHINE_DIR_KEY + " is not set.");
        for (File storageDir : storageDirList) {
            storagePathSupplierList.add(JavaUtils.memoize(() -> storageDir.toPath().resolve(getRaftPeerId().toString()).normalize().toAbsolutePath()));
        }

        fileInfoMap = new FileInfoMap(JavaUtils.memoize(() -> raftPeerIdSupplier.get() + ":files"));
    }

    private RaftPeerId getRaftPeerId() {
        return Objects.requireNonNull(raftPeerIdSupplier.get(), () -> JavaUtils.getClassSimpleName(getClass()) + " is not initialized.");
    }

    private Path getRoot(Path relative) {
        int index = relative.toAbsolutePath().toString().hashCode() % storagePathSupplierList.size();
        return storagePathSupplierList.get(Math.abs(index)).get();
    }

    public List<Path> getRoots() {
        List<Path> roots = new ArrayList<>();
        for (Supplier<Path> supplier : storagePathSupplierList) {
            roots.add(supplier.get());
        }
        return roots;
    }

    private Path getAbsPath(Path relative) throws IOException {
        Path dirPath = getRoot(relative);
        Path fileAbsPath = dirPath.resolve(relative).normalize().toAbsolutePath();

        if (fileAbsPath.equals(dirPath)) {
            throw new IOException("The file path " + relative + " resolved to " + fileAbsPath + " is the root directory " + dirPath);
        }

        if (!fileAbsPath.startsWith(dirPath)) {
            throw new IOException("The file path " + relative + " resolved to " + fileAbsPath + " is not a sub-path under root directory " + dirPath);
        }

        return fileAbsPath;
    }

    public CompletableFuture<ReadReplyProto> watch(String relative) {
        FileInfo info = fileInfoMap.watch(relative);
        ReadReplyProto reply = ReadReplyProto.newBuilder()
                .setResolvedPath(FileStoreCommon.toByteString(info.getRelativePath()))
                .build();
        if (info instanceof Watch) {
            return ((Watch) info).future.thenApply(uc -> reply);
        }
        return CompletableFuture.completedFuture(reply);
    }

    public CompletableFuture<ReadReplyProto> read(String relative,
                                                  long offset,
                                                  long length,
                                                  boolean readCommitted) {
        Supplier<String> name = () -> "read(" + relative + ", " + offset + ", " + length + ") @" + getRaftPeerId();

        CheckedSupplier<ReadReplyProto, IOException> task = LogUtils.newCheckedSupplier(
                LOG,
                () -> {
                    FileInfo fileInfo = fileInfoMap.get(relative);
                    ReadReplyProto.Builder readReplyProtoBuilder =
                            ReadReplyProto.newBuilder()
                                    .setResolvedPath(FileStoreCommon.toByteString(fileInfo.getRelativePath()))
                                    .setOffset(offset);

                    ByteString bytes = fileInfo.read(this::getAbsPath, offset, length, readCommitted);
                    return readReplyProtoBuilder.setData(bytes).build();
                },
                name);
        return submitTask(task, readThreadPool);
    }

    public CompletableFuture<Path> delete(long index, String relative) {
        Supplier<String> name = () -> "delete(" + relative + ") @" + getRaftPeerId() + ":" + index;
        CheckedSupplier<Path, IOException> deleteTask = LogUtils.newCheckedSupplier(LOG, () -> {
            FileInfo fileInfo = fileInfoMap.remove(relative);
            FileUtils.delete(getAbsPath(fileInfo.getRelativePath()));
            return fileInfo.getRelativePath();
        }, name);
        return submitTask(deleteTask, deleteThreadPool);
    }

    private <T> CompletableFuture<T> submitTask(CheckedSupplier<T, IOException> task,
                                                ExecutorService executorService) {
        CompletableFuture<T> f = new CompletableFuture<>();
        executorService.submit(() -> {
            try {
                f.complete(task.get());
            } catch (IOException e) {
                f.completeExceptionally(new IOException("Failed " + task, e));
            }
        });
        return f;
    }

    public CompletableFuture<WriteReplyProto> submitCommit(long index,
                                                           String relative,
                                                           boolean close,
                                                           long offset,
                                                           int size) {
        Function<UnderConstruction, ReadOnly> closeFunction = close ? fileInfoMap::close : null;
        UnderConstruction underConstruction;
        try {
            underConstruction = fileInfoMap.get(relative).asUnderConstruction();
        } catch (FileNotFoundException e) {
            return FileStoreCommon.completeExceptionally(index, "Failed to write to " + relative, e);
        }

        return underConstruction
                .submitCommitTask(offset, size, closeFunction, commitThreadPool, getRaftPeerId(), index)
                .thenApply(n -> WriteReplyProto.newBuilder()
                        .setResolvedPath(FileStoreCommon.toByteString(underConstruction.getRelativePath()))
                        .setOffset(offset)
                        .setLength(n)
                        .build());
    }

    public CompletableFuture<Integer> write(long index,
                                            String relative,
                                            boolean close,
                                            boolean sync,
                                            long offset,
                                            ByteString data) {
        int size = data != null ? data.size() : 0;

        LOG.info("write {}, offset={}, size={}, close? {} @{}:{}",
                relative, offset, size, close, getRaftPeerId(), index);

        boolean createNew = (offset == 0L);

        UnderConstruction underConstruction;
        if (createNew) {
            underConstruction = new UnderConstruction(FileStoreCommon.pathNormalize(relative));
            fileInfoMap.put(underConstruction);
        } else {
            try {
                underConstruction = fileInfoMap.get(relative).asUnderConstruction();
            } catch (FileNotFoundException e) {
                return FileStoreCommon.completeExceptionally(index, "Failed to write to " + relative, e);
            }
        }

        return size == 0 && !close ?
                CompletableFuture.completedFuture(0) : createNew ?
                underConstruction.create(this::getAbsPath, data, close, sync, writeThreadPool, getRaftPeerId(), index) :
                underConstruction.append(offset, data, close, sync, writeThreadPool, getRaftPeerId(), index);
    }

    @Override
    public void close() {
        writeThreadPool.shutdownNow();
        commitThreadPool.shutdownNow();
        readThreadPool.shutdownNow();
        deleteThreadPool.shutdownNow();
    }

    public CompletableFuture<StreamWriteReplyProto> streamCommit(String p, long bytesWritten) {
        return CompletableFuture.supplyAsync(() -> {
            try (RandomAccessFile file = new RandomAccessFile(getAbsPath(FileStoreCommon.pathNormalize(p)).toFile(), "r")) {
                long len = file.length();
                return StreamWriteReplyProto.newBuilder().setIsSuccess(len == bytesWritten).setByteWritten(len).build();
            } catch (IOException e) {
                throw new CompletionException("Failed to commit stream " + p + " with " + bytesWritten + " B.", e);
            }
        }, commitThreadPool);
    }

    public CompletableFuture<?> streamLink(DataStream dataStream) {
        return CompletableFuture.supplyAsync(() -> {
            if (dataStream == null) {
                return JavaUtils.completeExceptionally(new IllegalStateException("Null stream"));
            }

            if (dataStream.getDataChannel().isOpen()) {
                return JavaUtils.completeExceptionally(new IllegalStateException("DataStream: " + dataStream + " is not closed properly"));
            }

            return CompletableFuture.completedFuture(null);
        }, commitThreadPool);
    }

    public CompletableFuture<FileStoreDataChannel> createDataChannel(String p) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path full = getAbsPath(FileStoreCommon.pathNormalize(p));
                return new FileStoreDataChannel(full);
            } catch (IOException e) {
                throw new CompletionException("Failed to create " + p, e);
            }
        }, writeThreadPool);
    }

}
