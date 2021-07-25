package com.aliyun.adb.contest.index;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.core.FileSegment;
import com.aliyun.adb.contest.util.Bucket;
import com.aliyun.adb.contest.util.Logger;
import com.aliyun.adb.contest.write.WriteManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author hum
 */
public class IndexBuilderRunner implements Runnable {

    private static final Logger logger = Logger.GLOBAL_LOGGER;

//    private final ByteBuffer localBuffer = ByteBuffer.allocate(1024);

    private final RandomAccessFile raf;
    private static long currentOffset;
    private int id;
    private final int colId = 0;
    private final int nextColId = 1;
    static long fileSize;
    final private WriteManager writeManager;
    int[][] bucketCount;

    public IndexBuilderRunner(int id, WriteManager writeManager, File currentFile) throws FileNotFoundException {
        this.id = id;
        bucketCount = Bucket.bucketCounts[id];
        this.writeManager = writeManager;
        raf = new RandomAccessFile(currentFile, "r");
        fileSize = currentFile.length();
    }

    private static synchronized FileSegment getNextSegment() throws IOException {
        if (currentOffset >= fileSize) {
            return null;
        }
        long prevOffset = currentOffset;
        currentOffset += Constants.SEGMENT_SIZE;
        return new FileSegment(prevOffset, Math.min(currentOffset, fileSize));
    }

    @Override
    public void run() {
        try {
            logger.info("thread %d into run...", id);
            FileSegment segment;
            while ((segment = getNextSegment()) != null) {
                readSegment(segment);
            }
            logger.info("thread %d run done...", id);
        } catch (Throwable ex) {
            logger.error("exception caught during IndexBuilder running", ex);
        }
    }

    private final byte[] readBuffer = new byte[Constants.SEGMENT_SIZE + Constants.SEGMENT_MARGE];
    int pos = 0;


    private void readSegment(FileSegment segment) throws IOException {
//        int columnLen = EnvInfo.size;

        pos = 0;

//        int limit;
//        long size = Math.min(Constants.SEGMENT_SIZE + Constants.SEGMENT_MARGE, fileSize - segment.getOffset());
//        MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, segment.getOffset(), size);
        raf.seek(segment.getOffset());
        int limit = raf.read(readBuffer);
//        limit = mbb.get(readBuffer, 0, (int) size).limit();
//        if (limit == -1) {
//            logger.info("end");
//        }
        while (pos < limit && readBuffer[pos++] != '\n') ;
        // +1 means while can reach '='
        long range = Math.min(segment.getNextOffset() - segment.getOffset() + 1, limit);
        while (pos < range) {
//            for (int i = 0; i < columnLen - 1; i++) {
            calColumnBucketCounts(colId, ',');
//            }
            calColumnBucketCounts(nextColId, '\n');
        }
//        id ^= Constants.CHANGE_THREAD_ID;
    }

    private void calColumnBucketCounts(int columnId, char c) {
        int startPosition = pos;
        int len = 19;
        while (readBuffer[startPosition + len] != c) {
            len--;
        }
        pos = startPosition + len;

//        long num = 0;
//        while (readBuffer[pos] != c) {
//            num = num * 10 + readBuffer[pos] - '0';
//            pos++;
//        }
//
//        // -1 because c
//        int len = pos - startPosition;
//        pos++;

        if (len == 19) {
            int bucketKey = Bucket.encode(readBuffer[startPosition++], readBuffer[startPosition++]);
            bucketCount[columnId][bucketKey]++;
            writeManager.putMessage(id, columnId, bucketKey, readBuffer, startPosition, pos);
        } else if (len == 18) {
            int bucketKey = Bucket.encode(readBuffer[startPosition++]);
            bucketCount[columnId][bucketKey]++;
            writeManager.putMessage(id, columnId, bucketKey, readBuffer, startPosition, pos);
        } else {
            bucketCount[columnId][0]++;
            writeManager.putMessage(id, columnId, 0, readBuffer, startPosition, pos);
        }
        pos++;
    }
}

