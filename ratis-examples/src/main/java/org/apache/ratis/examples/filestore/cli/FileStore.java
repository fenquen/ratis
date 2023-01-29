package org.apache.ratis.examples.filestore.cli;

import org.apache.ratis.examples.common.SubCommand;

import java.util.ArrayList;
import java.util.List;

public abstract class FileStore {
    public static List<SubCommand> getSubCommands() {
        List<SubCommand> subCommandList = new ArrayList<>();
        subCommandList.add(new FileStoreServer());
        subCommandList.add(new LoadGen());
        subCommandList.add(new DataStream());
        return subCommandList;
    }
}
