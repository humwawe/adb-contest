package com.aliyun.adb.contest.write;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.util.Convert;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author hum
 */
public class WriteByteBucket {
    private final RandomAccessFile raf;

    private final int bufNum = 2;

    private final byte[][] buffers = new byte[bufNum][Constants.WRITE_BUFFER_SIZE];
    private final Future[] futures = new Future[bufNum];

    private int index = 0;
    private int curOffset = 0;
    private final ExecutorService executorService;


    public WriteByteBucket(int threadId, RandomAccessFile randomAccessFile) {
        this.raf = randomAccessFile;
        executorService = WriteExecutor.getExecutorService(threadId);
        for (int i = 0; i < bufNum; i++) {
            futures[i] = executorService.submit(() -> {
            });
        }
    }

    public void putMessage(byte[] message, int startPosition, int pos) {
        if (curOffset == Constants.WRITE_BUFFER_SIZE) {
            int newIndex = (index + 1) & 1;
            if (!futures[newIndex].isDone()) {
                System.out.println("data block");
                try {
                    futures[newIndex].get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            futures[index] = executorService.submit(() -> {
                try {
                    raf.write(buffers[index]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            index = newIndex;
            curOffset = 0;
        }
        addLong(Convert.byte2long(message, startPosition, pos));
    }

    public void putLong(long num) {
        if (curOffset == Constants.WRITE_BUFFER_SIZE) {
            int newIndex = (index + 1) & 1;
            if (!futures[newIndex].isDone()) {
                System.out.println("data block");
                try {
                    futures[newIndex].get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            futures[index] = executorService.submit(() -> {
                try {
                    raf.write(buffers[index]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            index = newIndex;
            curOffset = 0;
        }
        addLong(num);
    }

    private void addLong(long v) {
        byte[] buffer = buffers[index];
        buffer[curOffset++] = (byte) (v >>> 56);
        buffer[curOffset++] = (byte) (v >>> 48);
        buffer[curOffset++] = (byte) (v >>> 40);
        buffer[curOffset++] = (byte) (v >>> 32);
        buffer[curOffset++] = (byte) (v >>> 24);
        buffer[curOffset++] = (byte) (v >>> 16);
        buffer[curOffset++] = (byte) (v >>> 8);
        buffer[curOffset++] = (byte) v;
    }

    public void close() throws IOException {
        try {
            for (Future future : futures) {
                if (!future.isDone()) {
                    future.get();
                }
            }
            if (curOffset > 0) {
                raf.write(buffers[index], 0, curOffset);
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        } finally {
            raf.close();
        }
    }
}

