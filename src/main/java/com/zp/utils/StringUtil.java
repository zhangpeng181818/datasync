package com.zp.utils;

import com.alibaba.datax.core.Engine;
import lombok.extern.slf4j.Slf4j;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class StringUtil {

    public static int batchSize = 8192;
    public static int batchByteSize = 33554432;

    private static final ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMdd");
        }
    };

    private static final ThreadLocal<DateFormat> timeFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("HHmm");
        }
    };

    public static String replace(String str) {
        return str.replaceAll("\\[", "").replaceAll("\\]", "");
    }

    public static String getCurrentClasspath() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String currentClasspath = classLoader.getResource("").getPath();
        // 当前操作系统
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            // 删除path中最前面的/
            currentClasspath = currentClasspath.substring(1);
        }
        return currentClasspath;
    }

    public static void exeDatax(String filePath) {
        String[] datxArgs = {"-job", filePath, "-mode", "standalone", "-jobid", "-1"};
        try {
            Engine.entry(datxArgs);
        } catch (Throwable e) {
            log.error("{}", e.getMessage(), e);
        }
    }

    public static String getFormatDate(String format, Date date) {
        if ("yyyyMMdd".equals(format)) {
            return dateFormat.get().format(date);
        } else if ("HHmm".equals(format)) {
            return timeFormat.get().format(date);
        }
        return "";
    }
}
