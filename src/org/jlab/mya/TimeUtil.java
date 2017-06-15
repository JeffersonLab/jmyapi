package org.jlab.mya;

import java.time.Instant;

/**
 * Utilities for conversion between Mya's database time format and Java's native Instant.
 *
 * Mya stores each timestamp in the database as a 64-bit field where the top half of the field is
 * UNIX time and the bottom half is fractional seconds.
 *
 * @author slominskir
 */
public final class TimeUtil {

    /**
     * Instantiating one of these is useless so don't do it.
     */
    private TimeUtil() {
        // Private constructor
    }

    /**
     * Convert a java.time.Instant to a Mya timestamp.
     * 
     * @param instant The Instant
     * @return The Mya timestamp
     */
    public static long toMyaTimestamp(Instant instant) {
        int lo; // Integer to fill lower bits
        int hi; // Integer to fill upper bits
        hi = (int) instant.getEpochSecond(); // Hope this part doesn't overflow...
        lo = instant.getNano();
        long timestamp = (((long) hi) << 32) | (lo & 0xffffffffL);
        return timestamp;
    }

    /**
     * Convert a Mya timestamp to a java.time.Instant.
     * 
     * @param timestamp The Mya timestamp
     * @return The Instant
     */
    public static Instant fromMyaTimestamp(long timestamp) {
        int lo;
        int hi;
        hi = (int) (timestamp >> 32); // >> 32 means sign extend; >>> 32 means zero-fill...
        lo = (int) timestamp;
        Instant instant = Instant.ofEpochSecond(hi, lo);
        return instant;
    }
}
