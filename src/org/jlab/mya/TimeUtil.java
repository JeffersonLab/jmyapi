package org.jlab.mya;

import java.time.Instant;

/**
 * Utility methods for conversion between Mya's database time format and Java's native Instant.
 *
 * Mya stores each timestamp in the database as a 64-bit field where the top half of the field is
 * UNIX time and the bottom half is fractional seconds.
 *
 * The fractional seconds are not something straight forward like the number of nano-seconds.
 * Instead they are treated similar to how NTP handles fractional seconds: 1 second = 1 / 2^32
 * fractional seconds.
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
        //hi = (int) instant.getEpochSecond(); // Hope this part doesn't overflow...
        hi = Math.toIntExact(instant.getEpochSecond()); // throws ArithmeticException if overflow
        lo = instant.getNano();
        long tmp = Integer.toUnsignedLong(lo);
        tmp = (long) (tmp * 4.294967296); // 4.294967296 = 1 / 0.23283064365386962890625
        lo = (int) tmp; // TODO: why is this okay down to microsecond, but not nanoseconds?
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

        lo = (int) timestamp; // cast will truncate to lowest 32 bits.

        // Java has no unsigned types, but we can use a long to hold the value of an unsigned integer
        long tmp = Integer.toUnsignedLong(lo); // this is expensive though... is there a better way to do the unsigned arithmetic?

        // 0.23283064365387 (0.23283064365386962890625) {10^9/2^32} is a scaling factor to convert to nanoseconds
        tmp = (long) (tmp * 0.23283064365387);

        lo = Math.toIntExact(tmp); // Throw ArithmeticException if overflow (unlike with cast)

        // TODO: for performance remove sanity checks and use cast instead of toIntExact?
        // Sanity checks - make sure nanoseconds are non-negative and less than a second worth.
        if (lo < 0) {
            throw new ArithmeticException("Underflow: negative nanoseconds");
        }

        if (lo > 999999999) {
            throw new ArithmeticException("Overflow: nanoseconds forming seconds");
        }

        Instant instant = Instant.ofEpochSecond(hi, lo);
        return instant;
    }
}
