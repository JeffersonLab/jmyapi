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
    private final PvEventType code;
    private final T[] value;

    PvRecord(Instant timestamp, PvEventType code, T[] value) {
        this.timestamp = timestamp;
        this.code = code;
        this.value = value;
    }

    @Override
    public String toString() {
        return "PvRecord{" + "timestamp=" + timestamp.atZone(ZoneId.systemDefault()) + ", code=" + code + ", value=" + value[0] + '}';
    }

    public String toColumnString() {
        return timestamp.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + " " + String.format("%32s", code) + " " + String.format("%24s", value[0]);
    }
}
