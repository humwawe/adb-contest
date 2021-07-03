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

}
