package com.aliyun.adb.contest.util;

/**
 * @author hum
 */
public class Convert {
    public static String tableColumnKey(String table, String column) {
        return (table + "." + column).toLowerCase();
    }

    public static String tableColumnEncodeKeyString(int idx, int key) {
        return idx + "" + key;
    }

    public static int tableColumnEncodeKeyInteger(int idx, int key) {
        return idx * 10000 + key;
    }

    public static String threadTableColumnBucket(int threadId, int columnId, int bucketKey) {
        return threadId + "." + columnId + "." + bucketKey;
    }

    public static long byte2long(byte[] data) {
        long res = 0;
        for (byte d : data) {
            res = res * 10 + d - '0';
        }
        return res;
    }

    public static byte[] copyOfRange(byte[] original, int from, int to) {
        int newLength = to - from;
        byte[] copy = new byte[newLength];
        System.arraycopy(original, from, copy, 0, newLength);
        return copy;
    }

    public static long byte2long(byte[] message, int startPosition, int pos) {
        long res = 0;
        for (int i = startPosition; i < pos; i++) {
            res = res * 10 + (message[i] - '0');
        }
        return res;
    }

    public static long kth2FinalKthLong(long kth, int bucketKey) {
        return Long.parseLong(kth2FinalKthString(kth, bucketKey));
    }

    public static String kth2FinalKthString(long kth, int bucketKey) {
        int len = 0;
        if (bucketKey > 9) {
            len = 19;
        } else if (bucketKey > 0) {
            len = 18;
        }
        String s = String.valueOf(kth);
        String ret;
        if (len == 19) {
            while (s.length() < len - 2) {
                s = "0" + s;
            }
            ret = "" + bucketKey + s;
        } else if (len == 18) {
            while (s.length() < len - 1) {
                s = "0" + s;
            }
            ret = "" + bucketKey + s;
        } else {
            ret = s;
        }
        return ret;
    }
}
