package com.aliyun.adb.contest;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.index.IndexAccumulator;
import com.aliyun.adb.contest.index.IndexBuilder;
import com.aliyun.adb.contest.index.IndexLoader;
import com.aliyun.adb.contest.index.IndexSaver;
import com.aliyun.adb.contest.query.LongReaderRunner;
import com.aliyun.adb.contest.spi.AnalyticDB;
import com.aliyun.adb.contest.util.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author hum
 */
public class HumAnalyticDB implements AnalyticDB {
    private static final Logger logger = Logger.GLOBAL_LOGGER;

    public HumAnalyticDB() {
        logger.info("HumAnalyticDB start");
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long start = System.currentTimeMillis();
        if (firstStage(workspaceDir)) {
            logger.info("first stage");
            logger.info("begin to load");

            initEnvInfo(tpchDataFileDir, workspaceDir);
            logger.info("end init inf");
//            logger.info(EnvInfo.printString());

            buildIndex();

            sumIndex();

            logger.info("begin to write index data");
            IndexSaver.saveIndex();
            logger.info("end write index data");

            logger.info("load stage time %d", System.currentTimeMillis() - start);

            logger.info("params are %s", Constants.printString());
        } else {
            logger.info("second stage");
            logger.info("begin to load index");
            IndexLoader.loadIndexData(workspaceDir);
        }

//        throw new RuntimeException("test");
    }


    private boolean firstStage(String workspaceDir) {
        File dir = new File(workspaceDir);
        File[] files = dir.listFiles();
        return files == null || files.length == 0;
    }

    private void sumIndex() {
        IndexAccumulator.sumIndex();
    }

    private void initEnvInfo(String tpchDataFileDir, String workspaceDir) throws IOException {
        EnvInfo.setEnvInfo(tpchDataFileDir, workspaceDir);
    }


    private void buildIndex() throws InterruptedException, IOException {
        IndexBuilder indexBuilder = new IndexBuilder();
        indexBuilder.buildIndex();

    }

    //    @Override
//    public String quantile(String table, String column, double percentile) throws Exception {
//        QueryExecutor queryExecutor = new QueryExecutor(table, column, percentile);
//        queryExecutor.queryFile();
//        return queryExecutor.getResult();
//    }
    ExecutorService executorService;

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        logger.info("Start quantile table: %s, column: %s, percentile %f ", table, column, percentile);
        long rank = Math.round(IndexAccumulator.sum * percentile);
        logger.info("rank: " + rank);
        int columnIndex = EnvInfo.tableColumn2Index.get(Convert.tableColumnKey(table, column));
        int bucketKey = SearchUtil.lowerBound(IndexAccumulator.bucketCounts[columnIndex], rank);
        logger.info("bucketKey: " + bucketKey);
        long rankInBucket;
        long cnt;
        if (bucketKey == 0) {
            rankInBucket = rank;
            cnt = IndexAccumulator.bucketCounts[columnIndex][0];
        } else {
            rankInBucket = rank - IndexAccumulator.bucketCounts[columnIndex][bucketKey - 1];
            cnt = IndexAccumulator.bucketCounts[columnIndex][bucketKey] - IndexAccumulator.bucketCounts[columnIndex][bucketKey - 1];
        }
        logger.info("cnt: " + cnt);
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(Constants.WRITE_NUM_CORE);
        }
        CountDownLatch latch = new CountDownLatch(Constants.WRITE_NUM_CORE);
        long[] list = new long[(int) cnt];
        int sum = 0;
        for (int threadId = 0; threadId < Constants.WRITE_NUM_CORE; threadId++) {
            LongReaderRunner longReaderRunner = new LongReaderRunner(threadId, columnIndex, bucketKey, list, sum, latch);
            executorService.submit(longReaderRunner);
            for (int i = 0; i < Constants.READ_NUM_CORE / Constants.WRITE_NUM_CORE; i++) {
                sum += Bucket.bucketCounts[threadId + i * Constants.WRITE_NUM_CORE][columnIndex][bucketKey];
            }
        }
        latch.await();
        long kth = SortUtil.findKthLargest(list, (int) (rankInBucket - 1));
        logger.info("end quantile ");
        int len = 0;
        if (bucketKey > 9) {
            len = 19;
        } else if (bucketKey > 0) {
            len = 18;
        }
        String s = String.valueOf(kth);
        if (len == 19) {
            while (s.length() < len - 2) {
                s = "0" + s;
            }
            return "" + bucketKey + s;
        } else if (len == 18) {
            while (s.length() < len - 1) {
                s = "0" + s;
            }
            return "" + bucketKey + s;
        } else {
            return s;
        }
    }
}

