package org.jlab.mya.jmyapi;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author ryans
 * @param <T>
 */
public class PvRecord<T> {
    private final Instant timestamp;
    private final int code;
    private T[] value;

    PvRecord(Instant timestamp, int code) {
        this.timestamp = timestamp;
        this.code = code;
    }

    @Override
    public String toString() {
        return "PvRecord{" + "timestamp=" + timestamp.atZone(ZoneId.systemDefault()) + ", code=" + code + '}';
    }

    public String toColumnString() {
        return timestamp.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + " " + code;
    }
}
