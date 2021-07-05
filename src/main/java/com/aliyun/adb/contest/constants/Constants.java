package com.aliyun.adb.contest.constants;

/**
 * @author hum
 */
public interface Constants {

    int BUCKET_SIZE = 930;

    int SEGMENT_MARGE = 50;
    int SEGMENT_SIZE = 512 * 1024 - SEGMENT_MARGE;

    int READ_NUM_CORE = 6;
//    int READ_BUFFER_NUM = 1 << 6;

    int WRITE_BUFFER_SIZE = 96 * 1024;

    int WRITE_NUM_CORE = 3;


//    int BUCKET_MEMO_SEPARATE = 10;
//    int BUCKET_MEMO_MARGE = 50000;
//    int BUCKET_MEMO_SIZE = 3600000 / READ_NUM_CORE + BUCKET_MEMO_MARGE;

    int COMPUTE_NUM_CORE = 6;

    // local 50000000 ,  prod  1000000000;
    int RECORD_SUM = 50000000;


    int COLUMN_NUM = 2;
    String INDEX_DATA = "index_data";
    String BUCKET_DATA = "bucket_data";
    String ENV_INFO = "env_info";
    //    int CHANGE_THREAD_ID = COMPUTE_NUM_CORE - 1;

    static String printString() {
        return "\nSEGMENT_SIZE: " + SEGMENT_SIZE
                + "\nREAD_NUM_CORE: " + READ_NUM_CORE
                + "\nWRITE_NUM_CORE: " + WRITE_NUM_CORE
                + "\nWRITE_BUFFER_SIZE: " + WRITE_BUFFER_SIZE
                + "\nCOMPUTE_NUM_CORE:" + COMPUTE_NUM_CORE;
//                + "\nREAD_BUFFER_NUM: " + READ_BUFFER_NUM;
    }
}
