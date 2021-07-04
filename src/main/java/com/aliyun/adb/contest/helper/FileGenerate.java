package com.aliyun.adb.contest.helper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hum
 */
public class FileGenerate {
    int N = 5000;

    void genBigFile(String dataFile, String bigDataFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(dataFile)));
        String s = reader.readLine();
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(bigDataFile));
//        bufferedWriter.write(s + "\n");
        bufferedWriter.write("O_ORDERKEY,O_CUSTKEY\n");
        String rawRow;
        List<String> list = new ArrayList<>();
        while ((rawRow = reader.readLine()) != null) {
            list.add(rawRow);
        }
        for (int i = 0; i < N; i++) {
            for (String s1 : list) {
                bufferedWriter.write(s1 + "\n");
            }
        }
        bufferedWriter.close();
    }
}
