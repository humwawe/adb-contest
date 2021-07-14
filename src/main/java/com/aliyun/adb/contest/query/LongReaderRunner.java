package com.aliyun.adb.contest.query;

import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.util.Convert;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;

/**
 * @author hum
 */
public class LongReaderRunner implements Runnable {
    private FileChannel fileChannel;
    long[] list;
    int start;
    CountDownLatch latch;
    boolean inMemo;

    public LongReaderRunner(int threadId, int columnIndex, int bucketKey, long[] list, int start, CountDownLatch latch) throws FileNotFoundException {
        fileChannel = new RandomAccessFile(new File(EnvInfo.workspace, Convert.threadTableColumnBucket(threadId, columnIndex, bucketKey)), "r").getChannel();
        this.list = list;
        this.start = start;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 16);
            while (fileChannel.read(byteBuffer) != -1) {
                byteBuffer.flip();
                while (byteBuffer.hasRemaining()) {
                    list[start++] = byteBuffer.getLong();
                }
                byteBuffer.clear();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        latch.countDown();
    }
}
