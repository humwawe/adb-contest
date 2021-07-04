package com.aliyun.adb.contest;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * @author hum
 */
public class HumAnalyticDBTest extends TestCase {

    String bigDataFileDir = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_big_data";
    String workspaceDir = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "workspace";
    String resultsFile = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_result" + File.separator + "results";

    @Test
    public void testdb() throws Exception {
        clean(workspaceDir);
        try {
            HumAnalyticDB demoAnalyticDB = new HumAnalyticDB();
            demoAnalyticDB.load(bigDataFileDir, workspaceDir);
            BufferedReader reader = new BufferedReader(new FileReader(resultsFile));
            String rawRow;
            while ((rawRow = reader.readLine()) != null) {
                String[] row = rawRow.split(" ");
                Assert.assertEquals(row[3], demoAnalyticDB.quantile(row[0], row[1], Double.parseDouble(row[2])));
                System.out.println(rawRow);
            }
            System.out.println("end query");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("clean files");
        }

    }

    private void clean(String workspaceDir) {
        File[] files = new File(workspaceDir).listFiles();
        for (File file : files) {
            file.delete();
        }
    }
}
