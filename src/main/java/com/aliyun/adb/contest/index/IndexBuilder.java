package com.aliyun.adb.contest.index;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.util.Logger;
import com.aliyun.adb.contest.write.WriteExecutor;
import com.aliyun.adb.contest.write.WriteManager;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hum
 */
public class IndexBuilder {
    private static final Logger logger = Logger.GLOBAL_LOGGER;

    public IndexBuilder() {
    }

    public void buildIndex() throws InterruptedException, IOException {
        logger.info("begin to build index");
        ExecutorService executorService = Executors.newFixedThreadPool(Constants.READ_NUM_CORE);

        for (int i = 0; i < Constants.READ_NUM_CORE; i++) {
            executorService.submit(new IndexBuilderRunner(i, WriteManager.getInstance(), EnvInfo.dataFiles[0]));
        }
//        for (int i = 0; i < Constants.READ_NUM_CORE; i++) {
//            executorService.submit(new ReadRunner(i, ComputeManager.getInstance(), EnvInfo.dataFiles[0]));
//        }


//        RandomAccessFile raf = new RandomAccessFile(EnvInfo.dataFiles[0], "r");
//        long fileSize = raf.length();
//
//        long segmentSize = fileSize / Constants.READ_NUM_CORE;
//
//        byte[] bytes = new byte[64];
//        raf.read(bytes);
//        int pos = 0;
//        while (bytes[pos++] != '\n') ;
//        long prevOffset = pos;
//        for (int i = 0; i < Constants.READ_NUM_CORE - 1; i++) {
//            long nextOffset = prevOffset + segmentSize;
//            raf.seek(nextOffset);
//            raf.read(bytes);
//            pos = 0;
//            while (bytes[pos++] != '\n') ;
//            nextOffset += pos;
//            FileSegment fileSegment = new FileSegment(prevOffset, nextOffset - 1);
//            executorService.submit(new IndexBuilderSegmentRunner(i, WriteManager.getInstance(), EnvInfo.dataFiles[0], fileSegment));
//            prevOffset = nextOffset;
//        }
//        FileSegment fileSegment = new FileSegment(prevOffset, fileSize);
//        executorService.submit(new IndexBuilderSegmentRunner(Constants.READ_NUM_CORE - 1, WriteManager.getInstance(), EnvInfo.dataFiles[0], fileSegment));


        logger.info("Building index ...");

        executorService.shutdown();
        executorService.awaitTermination(100000, TimeUnit.MINUTES);

//        logger.info("start close ComputeManager ...");
//        ComputeManager.close();
//        logger.info("ComputeManager closed ...");


        logger.info("start close writeManager ...");

        WriteManager.getInstance().close();

        logger.info("writeManager closed ...");

        logger.info("start close writeExecutor ...");

        WriteExecutor.close();

        logger.info("writeExecutor closed  ...");

        logger.info("Index built");
    }


}
