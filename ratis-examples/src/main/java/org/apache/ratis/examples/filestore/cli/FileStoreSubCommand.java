package org.apache.ratis.examples.filestore.cli;

import org.apache.ratis.examples.common.SubCommand;
import org.apache.ratis.examples.filestore.client.DataStream;
import org.apache.ratis.examples.filestore.client.LoadGen;
import org.apache.ratis.examples.filestore.server.FileStoreServerSubCommand;

import java.util.ArrayList;
import java.util.List;

public abstract class FileStoreSubCommand {
    public static List<SubCommand> getSubCommands() {
        List<SubCommand> subCommandList = new ArrayList<>();
        subCommandList.add(new FileStoreServerSubCommand());
        subCommandList.add(new LoadGen());
        subCommandList.add(new DataStream());
        return subCommandList;
    }
}
