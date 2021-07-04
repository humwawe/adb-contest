package com.aliyun.adb.contest.helper;

import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class FileGenerateTest {
    String dataFile = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_data" + File.separator + "lineitem";
    String bigDataFile = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_big_data" + File.separator + "lineitem";
    String bigDataFile2 = "src" + File.separator + "main" + File.separator + "resources" + File.separator + "test_big_data" + File.separator + "orders";


    @Test
    public void testGenBigFile() throws IOException {
        FileGenerate fileGenerate = new FileGenerate();
        fileGenerate.genBigFile(dataFile, bigDataFile2);
    }
}