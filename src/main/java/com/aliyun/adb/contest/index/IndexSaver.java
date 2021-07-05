package com.aliyun.adb.contest.index;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.util.Bucket;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * @author hum
 */
public class IndexSaver {
    public static void saveIndex() throws IOException {
        saveEnvInfo();
        saveIndexAccumulator();
        saveBucket();
    }

    private static void saveBucket() throws IOException {
        File file = new File(EnvInfo.workspace, Constants.BUCKET_DATA);
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        int[][][] bucketCounts = Bucket.bucketCounts;
        ByteBuffer buffer = ByteBuffer.allocate(bucketCounts.length * bucketCounts[0].length * bucketCounts[0][0].length * 4);
        for (int[][] bucketCount : bucketCounts) {
            for (int j = 0; j < bucketCounts[0].length; j++) {
                for (int k = 0; k < bucketCounts[0][0].length; k++) {
                    buffer.putInt(bucketCount[j][k]);
                }
            }
        }
        buffer.flip();
        fileChannel.write(buffer);
        fileChannel.force(true);
        fileChannel.close();

    }

    private static void saveIndexAccumulator() throws IOException {
        File file = new File(EnvInfo.workspace, Constants.INDEX_DATA);
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        int[][] bucketCounts = IndexAccumulator.bucketCounts;
        ByteBuffer buffer = ByteBuffer.allocate(bucketCounts.length * bucketCounts[0].length * 4);
        for (int[] bucketCount : bucketCounts) {
            for (int j = 0; j < bucketCounts[0].length; j++) {
                buffer.putInt(bucketCount[j]);
            }
        }
        buffer.flip();
        fileChannel.write(buffer);
        fileChannel.force(true);
        fileChannel.close();
    }

    private static void saveEnvInfo() throws IOException {
        File file = new File(EnvInfo.workspace, Constants.ENV_INFO);
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));

        Map<String, Integer> tableColumn2Index = EnvInfo.tableColumn2Index;
        for (Map.Entry<String, Integer> stringIntegerEntry : tableColumn2Index.entrySet()) {
            bufferedWriter.write(stringIntegerEntry.getKey() + "\n");
            bufferedWriter.write(stringIntegerEntry.getValue() + "\n");
        }
        bufferedWriter.close();
    }
}
