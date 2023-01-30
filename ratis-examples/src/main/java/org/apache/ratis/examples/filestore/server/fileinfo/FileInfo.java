package org.apache.ratis.examples.filestore.server.fileinfo;

import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public abstract class FileInfo {
    public static final Logger LOG = LoggerFactory.getLogger(FileInfo.class);

    private final Path relativePath;

    public FileInfo(Path relativePath) {
        this.relativePath = relativePath;
    }

    public Path getRelativePath() {
        return relativePath;
    }

    public long getWriteSize() {
        throw new UnsupportedOperationException();
    }

    public long getCommittedSize() {
        throw new UnsupportedOperationException();
    }

    public UnderConstruction asUnderConstruction() {
        throw new UnsupportedOperationException();
    }

    public ByteString read(CheckedFunction<Path, Path, IOException> resolver,
                           long offset,
                           long length,
                           boolean readCommitted) throws IOException {
        if (readCommitted && offset + length > getCommittedSize()) {
            throw new IOException("Failed to read Committed: offset (=" + offset
                    + " + length (=" + length + ") > size = " + getCommittedSize() + ", path=" + getRelativePath());
        }

        if (offset + length > getWriteSize()) {
            throw new IOException("Failed to read Wrote: offset (=" + offset
                    + " + length (=" + length + ") > size = " + getWriteSize() + ", path=" + getRelativePath());
        }

        try (SeekableByteChannel byteChannel = Files.newByteChannel(resolver.apply(relativePath), StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(FileStoreCommon.getChunkSize(length));
            byteChannel.position(offset).read(buffer);
            buffer.flip();
            return ByteString.copyFrom(buffer);
        }
    }
}
