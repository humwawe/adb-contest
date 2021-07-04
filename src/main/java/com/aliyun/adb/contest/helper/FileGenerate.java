package com.aliyun.adb.contest.helper;

import java.io.*;
import java.util.*;

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

    void genResult(String dataFile, String resultsFile) throws IOException {
        File file = new File(dataFile);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String s = reader.readLine();

        String table = "orders";
        String[] cols = new String[]{"O_ORDERKEY", "O_CUSTKEY"};
//        String table = file.getName();
//        String[] cols = s.split(",");

        Map<String, List<Long>> map = new HashMap<>();
        map.put(table + "." + cols[0], new ArrayList<>());
        map.put(table + "." + cols[1], new ArrayList<>());
        String rawRow;
        List<String> list = new ArrayList<>();
        while ((rawRow = reader.readLine()) != null) {
            String[] split = rawRow.split(",");
            for (int i = 0; i < 2; i++) {
                map.get(table + "." + cols[i]).add(Long.parseLong(split[i]));
            }
        }
        map.forEach((tableColumn, values) -> {
            values.sort(Long::compareTo);
            System.out.println("Finish loading column " + tableColumn);
        });
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resultsFile, true));
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 2; j++) {
                int rank = random.nextInt(10000);
                double p = (double) (rank + 1) / 10000;
                String res = table + " " + cols[j] + " " + p + " " + map.get(table + "." + cols[j]).get(rank);
                bufferedWriter.write(res + "\n");
            }
        }
        bufferedWriter.close();
    }
}
