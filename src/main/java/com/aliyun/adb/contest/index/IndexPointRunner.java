package com.aliyun.adb.contest.index;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.util.*;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hum
 */
public class IndexPointRunner implements Runnable {
    private static final Logger logger = Logger.GLOBAL_LOGGER;
//    private static final int split = Constants.SPLIT_START;
//    private static final int num = Constants.RECORD_SUM / split;
//    public static long[][] res = new long[4][num];
//    long[] list = new long[IndexAccumulator.maxBucketKeySize];
//    ExecutorService executorService = Executors.newFixedThreadPool(Constants.WRITE_NUM_CORE);
    int columnId;
    int index;


    public IndexPointRunner(int id) {
        this.columnId = id;
        index = 0;
    }


    @Override
    public void run() {
        logger.info("columnId %d into run...", columnId);
//        int rank = split;
//        while (index < num) {
//            int bucketKey = SearchUtil.lowerBound(IndexAccumulator.bucketCounts[columnId], rank);
//            int rankInBucket;
//            int cnt;
//            if (bucketKey == 0) {
//                rankInBucket = rank;
//                cnt = IndexAccumulator.bucketCounts[columnId][0];
//            } else {
//                rankInBucket = rank - IndexAccumulator.bucketCounts[columnId][bucketKey - 1];
//                cnt = IndexAccumulator.bucketCounts[columnId][bucketKey] - IndexAccumulator.bucketCounts[columnId][bucketKey - 1];
//            }
//            logger.info("columnId %d, rank:%d, bucketKey: %d, cnt: %d", columnId, rank, bucketKey, cnt);
//
//            try {
//                Bucket.getResList(columnId, bucketKey, executorService, list);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            logger.info("columnId %d, getResList end", columnId);
//            int start = rankInBucket;
////            int left = 0;
////            while (left + rankInBucket <= cnt) {
//////                long kth = Convert.kth2FinalKthLong(SortUtil.findKthLargest(list, rankInBucket - 1, left, cnt), bucketKey);
////                long kth = Convert.kth2FinalKthLong(SortUtil.quickSelect(list, rankInBucket, left, cnt - 1), bucketKey);
////                res[columnId][index++] = kth;
////                left += rankInBucket;
////                rankInBucket = split;
////            }
////            rank += rankInBucket + left - start;
//            Arrays.sort(list, 0, cnt);
//            while (rankInBucket <= cnt) {
//                res[columnId][index++] = Convert.kth2FinalKthLong(list[rankInBucket - 1], bucketKey);
//                rankInBucket += split;
//            }
//            rank += rankInBucket - start;
//            logger.info("columnId %d, get bucketKey hot point end", columnId);
//        }
//
//        executorService.shutdown();
//        try {
//            executorService.awaitTermination(100000, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        logger.info("columnId %d run done...", columnId);
    }

}
