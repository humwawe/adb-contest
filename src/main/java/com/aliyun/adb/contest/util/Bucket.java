package com.aliyun.adb.contest.util;

/**
 * @author hum
 */
public class Bucket {
    public static int[][][] bucketCounts;

    public static int encode(int byte1, int byte2) {
        return (byte1 - '0') * 10 + byte2 - '0';
    }

    public static int encode(int byte1) {
        return byte1 - '0';
    }

}
