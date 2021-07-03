package com.aliyun.adb.contest.write;

import com.aliyun.adb.contest.constants.Constants;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hum
 */
public class WriteExecutor {
    private static ExecutorService[] executorService = new ExecutorService[Constants.WRITE_NUM_CORE];
    static int index = 0;

    static {
        for (int i = 0; i < executorService.length; i++) {
            executorService[i] = Executors.newSingleThreadExecutor();
        }
    }

    public static ExecutorService getExecutorService(int idx) {
        return executorService[idx % executorService.length];
    }

    public static ExecutorService getExecutorService() {
        return executorService[index++ & 7];
    }

    public static void close() throws InterruptedException {
        for (ExecutorService service : executorService) {
            service.shutdown();
            service.awaitTermination(100000, TimeUnit.MINUTES);
        }
    }
}
