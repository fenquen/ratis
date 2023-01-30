package org.apache.ratis.examples.filestore.server.fileinfo;

import java.util.concurrent.CompletableFuture;

public class WriteInfo {

    /**
     * Future to make sure that each commit is executed after the corresponding write.
     */
    public final CompletableFuture<Integer> writeFuture;

    /**
     * Future to make sure that each commit is executed after the previous commit.
     */
    public final CompletableFuture<Integer> commitFuture;

    /**
     * Previous commit index.
     */
    public final long previousIndex;

    public WriteInfo(CompletableFuture<Integer> writeFuture, long previousIndex) {
        this.writeFuture = writeFuture;
        this.previousIndex = previousIndex;
        this.commitFuture = new CompletableFuture<>();
    }
}
