package com.aliyun.adb.contest.constants;

import com.aliyun.adb.contest.util.Bucket;
import com.aliyun.adb.contest.util.Convert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hum
 */
public class EnvInfo {
    public static String workspace;
    public static File[] dataFiles;
    public static Map<String, Integer> tableColumn2Index = new HashMap<>();
    public static Map<String, Integer> tableColumns = new HashMap<>();
    public static int size = 0;

    public static void setEnvInfo(String tpchDataFileDir, String workspaceDir) throws IOException {
        workspace = workspaceDir;
        File dir = new File(tpchDataFileDir);
        dataFiles = dir.listFiles();
        initMap();
        initBucket();

    }

    private static void initBucket() {
        Bucket.bucketCounts = new int[Constants.COMPUTE_NUM_CORE][size][Constants.BUCKET_SIZE];
    }

    private static void initMap() throws IOException {
        for (File dataFile : dataFiles) {
            BufferedReader reader = new BufferedReader(new FileReader(dataFile));
            String table = dataFile.getName();
            String[] columns = reader.readLine().split(",");
            tableColumns.put(table, columns.length);
            for (String column : columns) {
                tableColumn2Index.put(Convert.tableColumnKey(table, column), size++);
            }
        }
    }

    public static String printString() {
        return "EnvInfo{" +
                "size=" + size +
                ", workspace=" + workspace +
                ", dataFiles=" + Arrays.toString(dataFiles) +
                ", tableColumn2Index=" + tableColumn2Index +
                ", tablecolumns=" + tableColumns +
                ", size=" + size +
                '}';
    }
}