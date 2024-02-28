
package com.vortex.common.date;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.TimeZone;

/**
 * The SafeDateFormat actually is a proxy for joda DateTimeFormatter
 */
public class SafeDateFormat {

    private static final int ONE_HOUR_MS = 3600 * 1000;

    private final String pattern;
    private DateTimeFormatter formatter;

    public SafeDateFormat(String pattern) {
        this.pattern = pattern;
        this.formatter = DateTimeFormat.forPattern(pattern);
    }

    public synchronized void setTimeZone(String zoneId) {
        int hoursOffset = TimeZone.getTimeZone(zoneId).getRawOffset() /
                          ONE_HOUR_MS;
        DateTimeZone zone = DateTimeZone.forOffsetHours(hoursOffset);
        this.formatter = this.formatter.withZone(zone);
    }

    public TimeZone getTimeZome() {
        return this.formatter.getZone().toTimeZone();
    }

    public Date parse(String source) {
        return this.formatter.parseDateTime(source).toDate();
    }

    public String format(Date date) {
        return this.formatter.print(date.getTime());
    }

    public Object toPattern() {
        return this.pattern;
    }
}
