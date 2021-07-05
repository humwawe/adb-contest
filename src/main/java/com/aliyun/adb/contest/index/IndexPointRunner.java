package com.aliyun.adb.contest.index;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hum
 */
public class IndexPointRunner implements Runnable {
    private static final Logger logger = Logger.GLOBAL_LOGGER;
    private static final int split = Constants.SPLIT_START;
    private static final int num = Constants.RECORD_SUM / split;
    private static long[][] res = new long[4][num];

    ExecutorService executorService = Executors.newFixedThreadPool(Constants.WRITE_NUM_CORE);
    int columnId;
    int index;


    public IndexPointRunner(int id) {
        this.columnId = id;
        index = 0;
    }


    @Override
    public void run() {
        logger.info("columnId %d into run...", columnId);
        int rank = split;
        while (index < num) {
            int bucketKey = SearchUtil.lowerBound(IndexAccumulator.bucketCounts[columnId], rank);
            int rankInBucket;
            int cnt;
            if (bucketKey == 0) {
                rankInBucket = rank;
                cnt = IndexAccumulator.bucketCounts[columnId][0];
            } else {
                rankInBucket = rank - IndexAccumulator.bucketCounts[columnId][bucketKey - 1];
                cnt = IndexAccumulator.bucketCounts[columnId][bucketKey] - IndexAccumulator.bucketCounts[columnId][bucketKey - 1];
            }

            long[] list = new long[cnt];
            try {
                Bucket.getResList(columnId, bucketKey, executorService, list);
            } catch (Exception e) {
                e.printStackTrace();
            }
            int start = rankInBucket;
            int left = 0;
            while (left + rankInBucket <= cnt) {
                long kth = Convert.kth2FinalKthLong(SortUtil.findKthLargest(list, rankInBucket - 1, left, cnt), bucketKey);
                res[columnId][index++] = kth;
                left += rankInBucket;
            }
            rank += rankInBucket + left - start;
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(100000, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.info("columnId %d run done...", columnId);
    }

}
