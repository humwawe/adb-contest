package com.aliyun.adb.contest.helper;

import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class FileGenerateTest {
    String dataFile = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_data" + File.separator + "lineitem";
    String bigDataFile = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_big_data" + File.separator + "lineitem";
    String bigDataFile2 = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_big_data" + File.separator + "orders";
    String resultsFile = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_result" + File.separator + "more_results";

    @Test
    public void testGenBigFile() throws IOException {
        FileGenerate fileGenerate = new FileGenerate();
        fileGenerate.genBigFile(dataFile, bigDataFile2);
    }

    @Test
    public void testGenResult() throws IOException {
        FileGenerate fileGenerate = new FileGenerate();
        fileGenerate.genResult(dataFile, resultsFile);
    }
}