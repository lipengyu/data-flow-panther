package com.sxkj.utils;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
/**
 * @Author:zhengxiaofei
 * @Date:2019 /5/21 15:42
 * @Version 1.0
 * @Description flink以天为单位写入hdfs时，getBucketPath改为dt=
 */
public class DateTimeBucketer<T> implements Bucketer<T> {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";

    private final String formatString;

    private final ZoneId zoneId;

    private transient DateTimeFormatter dateTimeFormatter;

    /**
     * Creates a new {@code DateTimeBucketer} with format string {@code "yyyy-MM-dd--HH"} using JVM's default timezone.
     */
    public DateTimeBucketer() {
        this(DEFAULT_FORMAT_STRING);
    }

    /**
     * Creates a new {@code DateTimeBucketer} with the given date/time format string using JVM's default timezone.
     *
     * @param formatString The format string that will be given to {@code DateTimeFormatter} to determine
     *                     the bucket path.
     */
    public DateTimeBucketer(String formatString) {
        this(formatString, ZoneId.systemDefault());
    }

    /**
     * Creates a new {@code DateTimeBucketer} with format string {@code "yyyy-MM-dd--HH"} using the given timezone.
     *
     * @param zoneId The timezone used to format {@code DateTimeFormatter} for bucket path.
     */
    public DateTimeBucketer(ZoneId zoneId) {
        this(DEFAULT_FORMAT_STRING, zoneId);
    }

    /**
     * Creates a new {@code DateTimeBucketer} with the given date/time format string using the given timezone.
     *
     * @param formatString The format string that will be given to {@code DateTimeFormatter} to determine
     *                     the bucket path.
     * @param zoneId       The timezone used to format {@code DateTimeFormatter} for bucket path.
     */
    public DateTimeBucketer(String formatString, ZoneId zoneId) {
        this.formatString = Preconditions.checkNotNull(formatString);
        this.zoneId = Preconditions.checkNotNull(zoneId);

        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(zoneId);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        this.dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
    }

    @Override
    public Path getBucketPath(Clock clock, Path basePath, T element) {
        String newDateTimeString = dateTimeFormatter.format(Instant.ofEpochMilli(clock.currentTimeMillis()));
        return new Path(basePath + "/dt=" + newDateTimeString);
    }

    @Override
    public String toString() {
        return "DateTimeBucketer{" +
                "formatString='" + formatString + '\'' +
                ", zoneId=" + zoneId +
                '}';
    }
}

