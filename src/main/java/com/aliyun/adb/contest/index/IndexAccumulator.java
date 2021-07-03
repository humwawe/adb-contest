package com.aliyun.adb.contest.index;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.util.Bucket;
import com.aliyun.adb.contest.util.Logger;

/**
 * @author hum
 */
public class IndexAccumulator {
    private static final Logger logger = Logger.GLOBAL_LOGGER;
    public static long[][] bucketCounts;
    public static long sum;

    public static void sumIndex() {
        logger.info("begin to accumulate index");
        bucketCounts = new long[EnvInfo.size][Constants.BUCKET_SIZE];
        for (int columnId = 0; columnId < Bucket.bucketCounts[0].length; columnId++) {
            for (int bucketKey = 0; bucketKey < Bucket.bucketCounts[0][0].length; bucketKey++) {
                long tmp = 0;
                for (int threadId = 0; threadId < Bucket.bucketCounts.length; threadId++) {
                    tmp += Bucket.bucketCounts[threadId][columnId][bucketKey];
                }
                if (bucketKey == 0) {
                    bucketCounts[columnId][bucketKey] = tmp;
                } else {
                    bucketCounts[columnId][bucketKey] = tmp + bucketCounts[columnId][bucketKey - 1];
                }

            }
        }
        sum = bucketCounts[0][Constants.BUCKET_SIZE - 1];
        logger.info("record sum %d", sum);
        logger.info("Index data accumulated");
    }
}
