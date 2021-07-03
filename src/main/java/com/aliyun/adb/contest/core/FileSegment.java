package com.aliyun.adb.contest.core;

/**
 * @author hum
 */
public class FileSegment {

    private final long offset;
    private final long nextOffset;

    public FileSegment(long offset, long nextOffset) {
        this.offset = offset;
        this.nextOffset = nextOffset;
    }


    public long getOffset() {
        return offset;
    }

    public long getNextOffset() {
        return nextOffset;
    }
}
