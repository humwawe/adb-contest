package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author hum
 */
public class HumAnalyticDBTest extends TestCase {

    String bigDataFileDir = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_big_data";
    String workspaceDir = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "workspace";
    String resultsFile = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_result" + File.separator + "more_results";

    @Test
    public void testdb() throws Exception {
        clean(workspaceDir);
        File testDataDir = new File(bigDataFileDir);
        File testWorkspaceDir = new File(workspaceDir);
        File testResultsFile = new File(resultsFile);
        List<String> ans = new ArrayList<>();

        try (BufferedReader resReader = new BufferedReader(new FileReader(testResultsFile))) {
            String line;
            while ((line = resReader.readLine()) != null) {
                ans.add(line);
            }
        }

        // ROUND #1
        HumAnalyticDB analyticDB1 = new HumAnalyticDB();
        analyticDB1.load(testDataDir.getAbsolutePath(), testWorkspaceDir.getAbsolutePath());
        testQuery(analyticDB1, ans, 10);

        // To simulate exiting
        analyticDB1 = null;

        // ROUND #2
        HumAnalyticDB analyticDB2 = new HumAnalyticDB();
        analyticDB2.load(testDataDir.getAbsolutePath(), testWorkspaceDir.getAbsolutePath());

        Executor testWorkers = Executors.newFixedThreadPool(8);

        CompletableFuture[] futures = new CompletableFuture[8];

        for (int i = 0; i < 8; i++) {
            futures[i] = CompletableFuture.runAsync(() -> testQuery(analyticDB2, ans, 500), testWorkers);
        }

        CompletableFuture.allOf(futures).get();
    }

    private void testQuery(AnalyticDB analyticDB, List<String> ans, int testCount) {
        try {
            for (int i = 0; i < testCount; i++) {
                int p = ThreadLocalRandom.current().nextInt(ans.size());
//                int p = i % ans.size();
                String[] resultStr = ans.get(p).split(" ");
                String table = resultStr[0];
                String column = resultStr[1];
                double percentile = Double.parseDouble(resultStr[2]);
                String answer = resultStr[3];

                Assert.assertEquals(answer, analyticDB.quantile(table, column, percentile));
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void clean(String workspaceDir) {
        File[] files = new File(workspaceDir).listFiles();
        for (File file : files) {
            file.delete();
        }
    }
}
