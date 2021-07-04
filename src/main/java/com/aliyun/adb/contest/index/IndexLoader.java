package com.aliyun.adb.contest.index;

import com.aliyun.adb.contest.constants.Constants;
import com.aliyun.adb.contest.constants.EnvInfo;
import com.aliyun.adb.contest.util.Bucket;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hum
 */
public class IndexLoader {
    public static void loadIndexData(String workspaceDir) throws IOException {
        Map<String, Integer> tableColumn2Index = loadEnvInfo(workspaceDir);
        int[][] bucketCountsSum = loadIndexAccumulator(workspaceDir);
        int[][][] bucketCounts = loadBucket(workspaceDir);
        EnvInfo.tableColumn2Index = tableColumn2Index;
        IndexAccumulator.bucketCounts = bucketCountsSum;
        Bucket.bucketCounts = bucketCounts;
    }

    private static int[][][] loadBucket(String workspaceDir) throws IOException {
        File file = new File(workspaceDir, Constants.BUCKET_DATA);
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
        int[][][] bucketCounts = new int[Constants.COMPUTE_NUM_CORE][Constants.COLUMN_NUM * 2][Constants.BUCKET_SIZE];
        ByteBuffer buffer = ByteBuffer.allocateDirect(bucketCounts.length * bucketCounts[0].length * bucketCounts[0][0].length * 4);
        fileChannel.read(buffer);
        buffer.flip();
        for (int i = 0; i < bucketCounts.length; i++) {
            for (int j = 0; j < bucketCounts[0].length; j++) {
                for (int k = 0; k < bucketCounts[0][0].length; k++) {
                    bucketCounts[i][j][k] = buffer.getInt();
                }
            }
        }

        fileChannel.close();
        return bucketCounts;
    }

    private static int[][] loadIndexAccumulator(String workspaceDir) throws IOException {
        File file = new File(workspaceDir, Constants.INDEX_DATA);
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
        int[][] bucketCountsSum = new int[Constants.COLUMN_NUM * 2][Constants.BUCKET_SIZE];
        ByteBuffer buffer = ByteBuffer.allocate(bucketCountsSum.length * bucketCountsSum[0].length * 4);
        fileChannel.read(buffer);
        buffer.flip();
        for (int i = 0; i < bucketCountsSum.length; i++) {
            for (int j = 0; j < bucketCountsSum[0].length; j++) {
                bucketCountsSum[i][j] = buffer.getInt();
            }
        }

        fileChannel.close();
        return bucketCountsSum;
    }

    private static Map<String, Integer> loadEnvInfo(String workspaceDir) throws IOException {
        File file = new File(workspaceDir, Constants.ENV_INFO);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        Map<String, Integer> tableColumn2Index = new HashMap<>();
        String tableColumn;
        while ((tableColumn = reader.readLine()) != null) {
            int colId = Integer.parseInt(reader.readLine());
            tableColumn2Index.put(tableColumn, colId);
        }
        reader.close();
        return tableColumn2Index;
    }
}
