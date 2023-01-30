package org.apache.ratis.examples.filestore.server;

import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.examples.filestore.server.fileinfo.FileInfo;
import org.apache.ratis.examples.filestore.server.fileinfo.ReadOnly;
import org.apache.ratis.examples.filestore.server.fileinfo.UnderConstruction;
import org.apache.ratis.examples.filestore.server.fileinfo.Watch;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class FileInfoMap {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileInfo.class);
    private final Object name;

    private final Map<Path, FileInfo> path_fileInfo = new ConcurrentHashMap<>();

    public FileInfoMap(Supplier<String> name) {
        this.name = StringUtils.stringSupplierAsObject(name);
    }

    public FileInfo get(String relative) throws FileNotFoundException {
        return applyFunction(relative, path_fileInfo::get);
    }

    public FileInfo watch(String relative) {
        try {
            return applyFunction(relative, p -> path_fileInfo.computeIfAbsent(p, Watch::new));
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Failed to watch " + relative, e);
        }
    }

    public FileInfo remove(String relative) throws FileNotFoundException {
        LOGGER.trace("{}: remove {}", name, relative);
        return applyFunction(relative, path_fileInfo::remove);
    }

    private FileInfo applyFunction(String relative, Function<Path, FileInfo> f) throws FileNotFoundException {
        FileInfo fileInfo = f.apply(FileStoreCommon.pathNormalize(relative));
        if (fileInfo == null) {
            throw new FileNotFoundException("File " + relative + " not found in " + name);
        }

        return fileInfo;
    }

    public void put(UnderConstruction underConstruction) {
        LOGGER.trace("{}: putNew {}", name, underConstruction.getRelativePath());

        FileInfo previous = path_fileInfo.put(underConstruction.getRelativePath(), underConstruction);
        if (previous instanceof Watch) {
            ((Watch) previous).complete(underConstruction);
        }
    }

    /**
     * 使用readOnly替换underConstruction
     */
    public ReadOnly close(UnderConstruction underConstruction) {
        LOGGER.trace("{}: close {}", name, underConstruction.getRelativePath());

        ReadOnly readOnly = new ReadOnly(underConstruction);
        CollectionUtils.replaceExisting(underConstruction.getRelativePath(), underConstruction, readOnly, path_fileInfo, name::toString);
        return readOnly;
    }
}
