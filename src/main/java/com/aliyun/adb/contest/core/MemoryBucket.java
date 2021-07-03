package com.aliyun.adb.contest.core;

/**
 * @author hum
 */
public class MemoryBucket {
    long[] memo;
    int len;
    int size;

    public MemoryBucket(int size) {
        memo = new long[size];
        this.size = size;
        len = 0;
    }

    public void putLong(long num) {
        memo[len++] = num;
    }

    public int getLen() {
        return len;
    }

    public long getLong(int idx) {
        return memo[idx];
    }
}
