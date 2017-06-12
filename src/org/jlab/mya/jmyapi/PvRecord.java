package org.jlab.mya.jmyapi;

import java.time.Instant;

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
        return "PvRecord{" + "timestamp=" + timestamp + ", code=" + code + '}';
    }
}
