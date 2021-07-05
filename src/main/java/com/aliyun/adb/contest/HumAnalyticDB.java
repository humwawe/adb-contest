package com.aliyun.adb.contest;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.index.*;
import com.aliyun.adb.contest.spi.AnalyticDB;
import com.aliyun.adb.contest.util.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

            logger.info("begin to build index");
            buildIndex();
            logger.info("Index built");

            logger.info("begin to accumulate index");
            sumIndex();
            logger.info("Index data accumulated");

            logger.info("begin to accumulate hot point");
            getHotPoint();
            logger.info("hot point accumulated");

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

    private void getHotPoint() throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(EnvInfo.size);
        for (int colmunId = 0; colmunId < EnvInfo.size; colmunId++) {
            service.submit(new IndexPointRunner(colmunId));
        }
        service.shutdown();
        service.awaitTermination(100000, TimeUnit.MINUTES);

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
    ExecutorService executorService = Executors.newFixedThreadPool(12);

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Thread %d Start quantile table: %s, column: %s, percentile %f ", Thread.currentThread().getId(), table, column, percentile);
        long rank = Math.round(IndexAccumulator.sum * percentile);
        int columnIndex = EnvInfo.tableColumn2Index.get(Convert.tableColumnKey(table, column));

        int bucketKey = SearchUtil.lowerBound(IndexAccumulator.bucketCounts[columnIndex], rank);
        long rankInBucket;
        int cnt;
        if (bucketKey == 0) {
            rankInBucket = rank;
            cnt = IndexAccumulator.bucketCounts[columnIndex][0];
        } else {
            rankInBucket = rank - IndexAccumulator.bucketCounts[columnIndex][bucketKey - 1];
            cnt = IndexAccumulator.bucketCounts[columnIndex][bucketKey] - IndexAccumulator.bucketCounts[columnIndex][bucketKey - 1];
        }
        long[] list = new long[cnt];
        Bucket.getResList(columnIndex, bucketKey, executorService, list);

        long kth = SortUtil.findKthLargest(list, (int) (rankInBucket - 1));
        String res = Convert.kth2FinalKthString(kth, bucketKey);
        logger.info("rank: %d, bucketKey: %d, res: %s", rank, bucketKey, res);
        logger.info("end query  time: %d", System.currentTimeMillis() - start);
        return res;
    }
}

