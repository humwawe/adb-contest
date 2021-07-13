package com.aliyun.adb.contest.write;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.util.Convert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author hum
 */
public class WriterBucket {
    private final FileChannel fileChannel;

    private final int bufNum = 2;

    private final ByteBuffer[] buffers = new ByteBuffer[bufNum];
    private final Future[] futures = new Future[bufNum];
    private int index = 0;
    private final ExecutorService executorService;


    public WriterBucket(int threadId, int columnId, int bucketKey, FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        executorService = WriteExecutor.getExecutorService(threadId);
        for (int i = 0; i < bufNum; i++) {
            buffers[i] = ByteBuffer.allocateDirect(Constants.WRITE_BUFFER_SIZE);
            futures[i] = executorService.submit(() -> {
            });
        }
    }


    public void putMessage(byte[] message, int startPosition, int pos) {
        if (!buffers[index].hasRemaining()) {
            ByteBuffer tmpBuffer = buffers[index];
            int newIndex = (index + 1) & 1;
            tmpBuffer.flip();
            if (!futures[newIndex].isDone()) {
                System.out.println("data block");
                try {
                    futures[newIndex].get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            futures[index] = executorService.submit(() -> fileChannel.write(tmpBuffer));
//            try {
//                fileChannel.write(tmpBuffer);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            index = newIndex;
            buffers[index].clear();
        }
        buffers[index].putLong(Convert.byte2long(message, startPosition, pos));
//        put2Int(message, startPosition, pos);
    }

    private void put2Int(byte[] message, int startPosition, int pos) {
        int num = 0;
        int mid = pos + startPosition >> 1;
        for (int i = startPosition; i < mid; i++) {
            num = num * 10 + (message[i] - '0');
        }
        buffers[index].putInt(num);
        num = 0;
        for (int i = mid; i < pos; i++) {
            num = num * 10 + (message[i] - '0');
        }
        buffers[index].putInt(num);
    }

//    public void putLong(long num) {
//        if (!buffers[index].hasRemaining()) {
//            ByteBuffer tmpBuffer = buffers[index];
//            int newIndex = (index + 1) & 1;
//            tmpBuffer.flip();
//            if (!futures[newIndex].isDone()) {
//                System.out.println("data block");
//                try {
//                    futures[newIndex].get();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            futures[index] = executorService.submit(() -> fileChannel.write(tmpBuffer));
//            index = newIndex;
//            buffers[index].clear();
//        }
//        buffers[index].putLong(num);
//    }

    public void close() {
        try {
            for (Future future : futures) {
                if (!future.isDone()) {
                    future.get();
                }
            }
            if (buffers[index].hasRemaining()) {
                buffers[index].flip();
                fileChannel.write(buffers[index]);
                buffers[index].clear();
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }


}
