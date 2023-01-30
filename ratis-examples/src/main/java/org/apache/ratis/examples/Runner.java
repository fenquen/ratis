/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.ratis.examples.arithmetic.cli.Arithmetic;
import org.apache.ratis.examples.common.SubCommand;
import org.apache.ratis.examples.filestore.cli.FileStoreSubCommand;
import org.apache.ratis.util.JavaUtils;

import java.util.List;
import java.util.Optional;

public final class Runner {

    private Runner() {

    }

    /**
     * server.sh fileStore server --id n0 --storage tmp/n0 --peers n0:localhost:6000,n1:localhost:6001,n2:localhost:6002
     *
     * args[0] 要运行的功能的种类 fileStore
     * args[1] 运行 server 还是 loadgen(它是client)
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("No command type specified: ");
            return;
        }

        List<SubCommand> subCommandList = initializeCommands(args[0]);
        if (subCommandList == null) {
            System.err.println("Wrong command type: " + args[0]);
            return;
        }

        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, args.length - 1);

        Runner runner = new Runner();

        JCommander.Builder builder = JCommander.newBuilder().addObject(runner);
        subCommandList.forEach(command -> builder.addCommand(JavaUtils.getClassSimpleName(command.getClass()).toLowerCase(), command));
        JCommander jCommander = builder.build();

        try {
            jCommander.parse(newArgs);
            Optional<SubCommand> selectedCommand = subCommandList.stream()
                    .filter(command -> JavaUtils.getClassSimpleName(command.getClass()).equalsIgnoreCase(jCommander.getParsedCommand()))
                    .findFirst();
            if (selectedCommand.isPresent()) {
                selectedCommand.get().run();
            } else {
                jCommander.usage();
            }
        } catch (ParameterException exception) {
            System.err.println("Wrong parameters: " + exception.getMessage());
            jCommander.usage();
        }

    }

    private static List<SubCommand> initializeCommands(String command) {

        if (command.equalsIgnoreCase(JavaUtils.getClassSimpleName(FileStoreSubCommand.class))) {
            return FileStoreSubCommand.getSubCommands();
        }

        if (command.equalsIgnoreCase(JavaUtils.getClassSimpleName(Arithmetic.class))) {
            return Arithmetic.getSubCommands();
        }

        return null;
    }

}
