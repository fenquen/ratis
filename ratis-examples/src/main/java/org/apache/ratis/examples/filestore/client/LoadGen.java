package org.apache.ratis.examples.filestore.client;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Parameters(commandDescription = "Load Generator for FileStore")
public class LoadGen extends FileStoreClientSubCommand {

    @Parameter(names = {"--sync"}, description = "Whether sync every bufferSize")
    private int sync = 0;

    @Override
    protected void operation(List<FileStoreClient> fileStoreClientList) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(getNumThread());

        // 在客户端本地身成相应数量的文件
        List<String> generatedLocalFileAbsPathList = generateLocalFiles(executorService);

        FileStoreCommon.dropCache();

        System.out.println("Starting Async write now ");

        long startTime = System.currentTimeMillis();
        long totalWrittenBytes = waitWriteFinish(writeFiles(generatedLocalFileAbsPathList, fileStoreClientList, executorService));

        System.out.println("Total files written: " + getNumFiles());
        System.out.println("Each files size: " + getFileSizeInByte());
        System.out.println("Total data written: " + totalWrittenBytes + " bytes");
        System.out.println("Total time taken: " + (System.currentTimeMillis() - startTime) + " millis");

        stop(fileStoreClientList);
    }

    private Map<String, CompletableFuture<List<CompletableFuture<Long>>>> writeFiles(List<String> generatedLocalFileAbsPathList,
                                                                                     List<FileStoreClient> fileStoreClientList,
                                                                                     ExecutorService executorService) {
        Map<String, CompletableFuture<List<CompletableFuture<Long>>>> generatedLocalFileAbsPath_future = new HashMap<>();

        int clientIndex = 0;

        for (String generatedLocalFileAbsPath : generatedLocalFileAbsPathList) {
            CompletableFuture<List<CompletableFuture<Long>>> future = new CompletableFuture<>();

            FileStoreClient fileStoreClient = fileStoreClientList.get(clientIndex++ % fileStoreClientList.size());

            CompletableFuture.supplyAsync(() -> {
                List<CompletableFuture<Long>> futureList = new ArrayList<>();

                File generatedLocalFile = new File(generatedLocalFileAbsPath);

                try (FileInputStream fileInputStream = new FileInputStream(generatedLocalFile)) {
                    FileChannel fileChannel = fileInputStream.getChannel();
                    for (long offset = 0L; offset < fileSizeInByte; ) {
                        offset += writeBuffer(fileChannel, offset, fileStoreClient, generatedLocalFile.getName(), futureList);
                    }
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }

                future.complete(futureList);
                return future;
            }, executorService);

            generatedLocalFileAbsPath_future.put(generatedLocalFileAbsPath, future);
        }

        return generatedLocalFileAbsPath_future;
    }

    private long writeBuffer(FileChannel fileChannel,
                             long offset,
                             FileStoreClient fileStoreClient,
                             String path,
                             List<CompletableFuture<Long>> futureList) throws IOException {
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(bufferSizeInBytes);
        int byteRead = byteBuf.writeBytes(fileChannel, bufferSizeInBytes);

        if (byteRead < 0) {
            throw new IllegalStateException("Failed to read " + bufferSizeInBytes + " byte(s) from " + this + ", the channel has reached end-of-stream at " + offset);
        }

        if (byteRead > 0) {
            CompletableFuture<Long> future = fileStoreClient.writeAsync(path, offset, offset + byteRead == fileSizeInByte, byteBuf.nioBuffer(), sync == 1);
            future.thenRun(byteBuf::release);
            futureList.add(future);
        }

        return byteRead;
    }

    private long waitWriteFinish(Map<String, CompletableFuture<List<CompletableFuture<Long>>>> generatedLocalFileAbsPath_future) throws Exception {
        long totalByte = 0;
        for (CompletableFuture<List<CompletableFuture<Long>>> future : generatedLocalFileAbsPath_future.values()) {
            long writtenLen = 0;

            for (CompletableFuture<Long> future0 : future.get()) {
                writtenLen += future0.join();
            }

            if (writtenLen != fileSizeInByte) {
                System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInByte());
            }

            totalByte += writtenLen;
        }
        return totalByte;
    }
}
