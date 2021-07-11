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
//        loadHotPoint(workspaceDir);
        EnvInfo.workspace = workspaceDir;
        loadEnvInfo(workspaceDir);
        loadIndexAccumulator(workspaceDir);
        loadBucket(workspaceDir);
    }

//    private static void loadHotPoint(String workspaceDir) throws IOException {
//        File file = new File(workspaceDir, Constants.HOT_POINT);
//        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
//        int num = Constants.RECORD_SUM / Constants.SPLIT_START;
//        long[][] hotPoints = new long[4][num];
//        ByteBuffer buffer = ByteBuffer.allocate(hotPoints.length * hotPoints[0].length * 8);
//        fileChannel.read(buffer);
//        buffer.flip();
//        for (int i = 0; i < hotPoints.length; i++) {
//            for (int j = 0; j < hotPoints[0].length; j++) {
//                hotPoints[i][j] = buffer.getLong();
//            }
//        }
//        fileChannel.close();
//        IndexPointRunner.res = hotPoints;
//    }

    private static void loadBucket(String workspaceDir) throws IOException {
        File file = new File(workspaceDir, Constants.BUCKET_DATA);
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
        int[][][] bucketCounts = new int[Constants.READ_NUM_CORE][Constants.COLUMN_NUM * 2][Constants.BUCKET_SIZE];
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
        Bucket.bucketCounts = bucketCounts;
    }

    private static void loadIndexAccumulator(String workspaceDir) throws IOException {
        File file = new File(workspaceDir, Constants.INDEX_DATA);
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
        int[][] bucketCountsSum = new int[Constants.COLUMN_NUM * 2][Constants.BUCKET_SIZE];
        ByteBuffer buffer = ByteBuffer.allocate(bucketCountsSum.length * bucketCountsSum[0].length * 4 + 4);
        fileChannel.read(buffer);
        buffer.flip();
        IndexAccumulator.maxBucketKeySize = buffer.getInt();
        for (int i = 0; i < bucketCountsSum.length; i++) {
            for (int j = 0; j < bucketCountsSum[0].length; j++) {
                bucketCountsSum[i][j] = buffer.getInt();
            }
        }

        fileChannel.close();
        IndexAccumulator.bucketCounts = bucketCountsSum;

    }

    private static void loadEnvInfo(String workspaceDir) throws IOException {
        File file = new File(workspaceDir, Constants.ENV_INFO);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        Map<String, Integer> tableColumn2Index = new HashMap<>();
        String tableColumn;
        while ((tableColumn = reader.readLine()) != null) {
            int colId = Integer.parseInt(reader.readLine());
            tableColumn2Index.put(tableColumn, colId);
        }
        reader.close();
        EnvInfo.tableColumn2Index = tableColumn2Index;
    }
}
