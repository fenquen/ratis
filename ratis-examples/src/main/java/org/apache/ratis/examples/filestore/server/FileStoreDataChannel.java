package org.apache.ratis.examples.filestore.server;

import org.apache.ratis.statemachine.StateMachine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class FileStoreDataChannel implements StateMachine.DataChannel {

    private final RandomAccessFile randomAccessFile;

    public FileStoreDataChannel(Path fileAbsPath) throws FileNotFoundException {
        randomAccessFile = new RandomAccessFile(fileAbsPath.toFile(), "rw");
    }

    @Override
    public void force(boolean metadata) throws IOException {
        randomAccessFile.getChannel().force(metadata);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return randomAccessFile.getChannel().write(src);
    }

    @Override
    public boolean isOpen() {
        return randomAccessFile.getChannel().isOpen();
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }

}