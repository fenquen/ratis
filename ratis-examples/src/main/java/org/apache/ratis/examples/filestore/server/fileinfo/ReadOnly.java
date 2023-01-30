package org.apache.ratis.examples.filestore.server.fileinfo;

public class ReadOnly extends FileInfo {
    private final long committedSize;
    private final long writeSize;

    public ReadOnly(UnderConstruction underConstruction) {
        super(underConstruction.getRelativePath());
        committedSize = underConstruction.getCommittedSize();
        writeSize = underConstruction.getWriteSize();
    }

    @Override
    public long getCommittedSize() {
        return committedSize;
    }

    @Override
    public long getWriteSize() {
        return writeSize;
    }



}