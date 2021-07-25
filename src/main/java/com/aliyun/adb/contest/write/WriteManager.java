package com.aliyun.adb.contest.write;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.util.Convert;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

/**
 * @author hum
 */
public class WriteManager {
    private volatile static WriteManager instance;

    private final WriterBucket[][][] writerBuckets;
    private final FileChannel[][][] fileChannels;

    private WriteManager() throws FileNotFoundException {
        writerBuckets = new WriterBucket[Constants.READ_NUM_CORE][EnvInfo.size][Constants.BUCKET_SIZE];
//        memoryBuckets = new MemoryBucket[Constants.WRITE_NUM_CORE][EnvInfo.size][Constants.BUCKET_MEMO_SEPARATE];
        fileChannels = new FileChannel[Constants.WRITE_NUM_CORE][EnvInfo.size][Constants.BUCKET_SIZE];
        for (int i = 0; i < fileChannels.length; i++) {
            for (int j = 0; j < EnvInfo.size; j++) {
                for (int k = 0; k < Constants.BUCKET_SIZE; k++) {
//                    fileChannels[i][j][k] = new RandomAccessFile(new File(EnvInfo.workspace, Convert.threadTableColumnBucket(i, j, k)), "rw").getChannel();
                    fileChannels[i][j][k] = new FileOutputStream(new File(EnvInfo.workspace, Convert.threadTableColumnBucket(i, j, k))).getChannel();
                }
            }
        }
        for (int i = 0; i < writerBuckets.length; i++) {
            for (int j = 0; j < EnvInfo.size; j++) {
                for (int k = 0; k < Constants.BUCKET_SIZE; k++) {
                    writerBuckets[i][j][k] = new WriterBucket(i, j, k, getFileChannel(i, j, k));
                }
            }
        }

//        writerBuckets = new WriteByteBucket[Constants.COMPUTE_NUM_CORE][EnvInfo.size][Constants.BUCKET_SIZE];
//        randomAccessFiles = new RandomAccessFile[Constants.WRITE_NUM_CORE][EnvInfo.size][Constants.BUCKET_SIZE];
//        for (int i = 0; i < randomAccessFiles.length; i++) {
//            for (int j = 0; j < EnvInfo.size; j++) {
//                for (int k = 0; k < Constants.BUCKET_SIZE; k++) {
//                    randomAccessFiles[i][j][k] = new RandomAccessFile(EnvInfo.workspace + File.separator + Convert.threadTableColumnBucket(i, j, k), "rws");
//                }
//            }
//        }
//
//        for (int i = 0; i < writerBuckets.length; i++) {
//            for (int j = 0; j < EnvInfo.size; j++) {
//                for (int k = 0; k < Constants.BUCKET_SIZE; k++) {
//                    writerBuckets[i][j][k] = new WriteByteBucket(i, getRandomAccessFile(i, j, k));
//                }
////                for (int k = 0; k < Constants.BUCKET_MEMO_SEPARATE; k++) {
////                    memoryBuckets[i][j][k] = new MemoryBucket(Constants.BUCKET_MEMO_SIZE);
////                }
//            }
//        }

    }

    private FileChannel getFileChannel(int i, int j, int k) {
        return fileChannels[i % Constants.WRITE_NUM_CORE][j][k];
    }

//    private RandomAccessFile getRandomAccessFile(int i, int j, int k) {
//        return randomAccessFiles[i % Constants.WRITE_NUM_CORE][j][k];
//    }


    public static WriteManager getInstance() {
        if (instance == null) {
            synchronized (WriteManager.class) {
                if (instance == null) {
                    try {
                        instance = new WriteManager();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return instance;
    }


    public void putMessage(int threadId, int columnId, int bucketKey, byte[] message, int startPosition, int pos) {
//        writerBuckets[threadId][columnId][bucketKey].putMessage(message, startPosition, pos);
    }

//    public void putLong(int threadId, int columnId, int bucketKey, long num) {
//        writerBuckets[threadId][columnId][bucketKey].putLong(num);
//    }

    public void close() {
        for (WriterBucket[][] writerBucket : writerBuckets) {
            for (int j = 0; j < writerBuckets[0].length; j++) {
                for (int k = 0; k < writerBuckets[0][0].length; k++) {
                    writerBucket[j][k].close();
                }
            }
        }
    }

//    public void close() throws IOException {
//        for (WriteByteBucket[][] writerBucket : writerBuckets) {
//            for (int j = 0; j < writerBuckets[0].length; j++) {
//                for (int k = 0; k < writerBuckets[0][0].length; k++) {
//                    writerBucket[j][k].close();
//                }
//            }
//        }
//    }
//



}

