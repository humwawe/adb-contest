package com.aliyun.adb.contest.util;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author hum
 */
public class Logger {

    private final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private PrintStream out;

    private Logger() {
        out = System.out;
    }

    public static final Logger GLOBAL_LOGGER = new Logger();

    public synchronized void info(String format, Object... args) {
        out.println("[" + timeFormat.format(new Date()) + "] INFO  " + String.format(format, args));
    }

    public synchronized void warn(String format, Object... args) {
        out.println("[" + timeFormat.format(new Date()) + "] WARN  " + String.format(format, args));
    }

    public synchronized void error(String message, Throwable ex) {
        out.println(message);
        ex.printStackTrace(out);
    }
}
