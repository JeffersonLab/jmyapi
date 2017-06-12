package org.jlab.mya.jmyapi;

import java.time.Instant;

/**
 *
 * @author ryans
 */
public final class MyaUtil {

    private MyaUtil() {
        // Private constructor
    }

    public static long toMyaTimestamp(Instant instant) {
        int lo; // Integer to fill lower bits
        int hi; // Integer to fill upper bits
        hi = (int) instant.getEpochSecond(); // Hope this part doesn't overflow...
        lo = instant.getNano();
        long timestamp = (((long) hi) << 32) | (lo & 0xffffffffL);
        return timestamp;
    }

    public static Instant fromMyaTimestamp(long timestamp) {
        int lo;
        int hi;
        hi = (int) (timestamp >> 32); // >> 32 means sign extend; >>> 32 means zero-fill...
        lo = (int) timestamp;
        Instant instant = Instant.ofEpochSecond(hi, lo);
        return instant;
    }
}
