package org.apache.ratis.examples.filestore;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public abstract class FileStoreCommon {
    public static final String STATEMACHINE_PREFIX = "example.filestore.statemachine";

    public static final String STATEMACHINE_DIR_KEY = STATEMACHINE_PREFIX + ".dir";

    public static final String STATEMACHINE_WRITE_THREAD_NUM = STATEMACHINE_PREFIX + ".write.thread.num";

    public static final String STATEMACHINE_READ_THREAD_NUM = STATEMACHINE_PREFIX + ".read.thread.num";

    public static final String STATEMACHINE_COMMIT_THREAD_NUM = STATEMACHINE_PREFIX + ".commit.thread.num";

    public static final String STATEMACHINE_DELETE_THREAD_NUM = STATEMACHINE_PREFIX + ".delete.thread.num";

    public static final SizeInBytes MAX_CHUNK_SIZE = SizeInBytes.valueOf(64, TraditionalBinaryPrefix.MEGA);

    public static int getChunkSize(long suggestedSize) {
        return Math.toIntExact(Math.min(suggestedSize, MAX_CHUNK_SIZE.getSize()));
    }

    public static ByteString toByteString(Path p) {
        return ProtoUtils.toByteString(p.toString());
    }

    public static <T> CompletableFuture<T> completeExceptionally(long index, String message) {
        return completeExceptionally(index, message, null);
    }

    public static <T> CompletableFuture<T> completeExceptionally(long index, String message, Throwable cause) {
        return completeExceptionally(message + ", index=" + index, cause);
    }

    public static <T> CompletableFuture<T> completeExceptionally(String message, Throwable cause) {
        return JavaUtils.completeExceptionally(new IOException(message, cause));
    }

    public static Path pathNormalize(String path) {
        return Paths.get(path).normalize();
    }

    public static void dropCache() {
        String[] cmds = {"/bin/sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"};
        try {
            Process pro = Runtime.getRuntime().exec(cmds);
            pro.waitFor();
        } catch (Throwable t) {
            System.err.println("Failed to run command:" + Arrays.toString(cmds) + ":" + t.getMessage());
        }
    }
}
