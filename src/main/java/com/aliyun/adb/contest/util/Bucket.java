package com.aliyun.adb.contest.util;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.query.LongReaderRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * @author hum
 */
public class Bucket {
    public static int[][][] bucketCounts;

    public static int encode(int byte1, int byte2, int byte3) {
        int t1 = byte1 - '0';
        int t2 = byte2 - '0';
        int t3 = byte3 - '0';
        return (t1 << 6) + (t1 << 5) + (t1 << 2) + (t2 << 3) + (t2 << 1) + t3;
//        return (byte1 - '0') * 100 + (byte2 - '0') * 10 + (byte3 - '0');
    }

    public static int encode(int byte1, int byte2) {
        int t1 = byte1 - '0';
        int t2 = byte2 - '0';
        return (t1 << 3) + (t1 << 1) + t2;
//        return (byte1 - '0') * 10 + byte2 - '0';
    }

    public static int encode(byte byte1) {
        return byte1 - '0';
    }

    public static void getResList(int columnIndex, int bucketKey, ExecutorService executorService, long[] list) throws Exception {
        CountDownLatch latch = new CountDownLatch(Constants.WRITE_NUM_CORE);
        int sum = 0;
        for (int threadId = 0; threadId < Constants.WRITE_NUM_CORE; threadId++) {
            LongReaderRunner longReaderRunner = new LongReaderRunner(threadId, columnIndex, bucketKey, list, sum, latch);
            executorService.submit(longReaderRunner);
            for (int i = 0; i < Constants.READ_NUM_CORE / Constants.WRITE_NUM_CORE; i++) {
                sum += bucketCounts[threadId + i * Constants.WRITE_NUM_CORE][columnIndex][bucketKey];
            }
        }
        latch.await();
    }
}
