package com.aliyun.adb.contest.util;

/**
 * @author hum
 */
public class SearchUtil {
    public static int lowerBound(int[] bucketCount, long rank) {
        int l = 0;
        int r = bucketCount.length - 1;
        while (l < r) {
            int mid = l + r >> 1;
            if (bucketCount[mid] >= rank) {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        return l;
    }
}
