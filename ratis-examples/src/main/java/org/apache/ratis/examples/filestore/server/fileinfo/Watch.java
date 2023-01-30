package org.apache.ratis.examples.filestore.server.fileinfo;

import org.apache.ratis.util.Preconditions;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class Watch extends FileInfo {
    public final CompletableFuture<UnderConstruction> future = new CompletableFuture<>();

    public Watch(Path relativePath) {
        super(relativePath);
    }

    public void complete(UnderConstruction underConstruction) {
        Preconditions.assertTrue(getRelativePath().equals(underConstruction.getRelativePath()));
        future.complete(underConstruction);
    }
}
