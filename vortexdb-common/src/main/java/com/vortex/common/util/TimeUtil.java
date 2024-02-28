
package com.vortex.common.util;

import java.time.Duration;
import java.util.Date;

public final class TimeUtil {

    @SuppressWarnings("deprecation")
    public static long BASE_TIME = new Date(2017 - 1900, 10, 28).getTime();

    public static long timeGen() {
        return System.currentTimeMillis() - BASE_TIME;
    }

    public static long timeGen(Date date) {
        return date.getTime() - BASE_TIME;
    }

    public static long timeGen(long time) {
        return time - BASE_TIME;
    }

    public static long tillNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    public static String readableTime(long time) {
        if (time > 60 * 1000) {
            // Remove the milliseconds part
            time = time / 1000 * 1000;
        }
        Duration duration = Duration.ofMillis(time);
        return duration.toString()
                       .substring(2)
                       .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                       .toLowerCase();
    }
}
