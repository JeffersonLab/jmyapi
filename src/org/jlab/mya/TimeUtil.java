package org.jlab.mya;

import java.time.Instant;

/**
 * Utility methods for conversion between Mya's database time format and Java's
 * native Instant.
 *
 * Mya stores each timestamp in the database as a 64-bit field where the top
 * half of the field is UNIX time and the bottom half is fractional seconds.
 *
 * The fractional seconds are not something straight forward like the number of
 * nano-seconds. Instead they are treated similar to how NTP handles fractional
 * seconds: 1 second = 1 / 2^32 fractional seconds.
 *
 * Fractional seconds (floating point) stored in 32 bits cannot always be
 * converted to nanoseconds (integer) and back again to fractional seconds such
 * that the number you start with is the same as the one you end with. The C++
 * API uses a max precision of microseconds, but even then may not guarantee
 * invertability.
 *
 * @author slominskir
 */
public final class TimeUtil {

    /**
     * To Nanoseconds scaling factor.
     *
     * 0.23283064365387 (0.23283064365386962890625) {10^9/2^32} is a scaling
     * factor to convert from 2^32 fractional seconds to nanoseconds
     */
    private final static double TO_NANO_SCALER = 0.23283064365387;

    /**
     * From Nanoseconds scaling factor.
     *
     * 4.294967296 = 1 / 0.23283064365386962890625 (scaling factor to convert
     * from nanoseconds to 2^32 fractional seconds)
     */
    private final static double FROM_NANO_SCALER = 4.294967296;

    /**
     * Bit mask for clearing high 32-bits of 64-bit long leaving only low
     * 32-bits.
     */
    private final static long HI_MASK = (1L << 32) - 1;

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

        // This is a slower way to do the above
        //hi = Math.toIntExact(instant.getEpochSecond()); // throws ArithmeticException if overflow
        lo = instant.getNano();

        // Java has no unsigned types, but we can use a long to hold the value of an unsigned integer              
        long unsignedLo = Integer.toUnsignedLong(lo);
        unsignedLo = (long) (unsignedLo * FROM_NANO_SCALER);
        lo = (int) unsignedLo;

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

        // Java has no unsigned types, but we can use a long to hold the value of an unsigned integer        
        long unsignedLo = timestamp & HI_MASK;

        // This is a slightly slower way to do the above
        //lo = (int) timestamp; // cast will truncate to lowest 32 bits.
        //long unsignedLo = Integer.toUnsignedLong(lo);
        unsignedLo = (long) (unsignedLo * TO_NANO_SCALER);

        lo = (int) unsignedLo;

        // This is a slightly slower way to do the above
        //lo = Math.toIntExact(tmp); // Throw ArithmeticException if overflow (unlike with cast)
        // Unnecessary sanity checks
        /*if (lo < 0) {
            throw new ArithmeticException("Underflow: negative nanoseconds");
        }

        if (lo > 999999999) {
            throw new ArithmeticException("Overflow: nanoseconds forming seconds");
        }*/
        Instant instant = Instant.ofEpochSecond(hi, lo);
        return instant;
    }

    /**
     * Obtain a DateTimeFormatter pattern string with the specified number of 
     * fractional seconds.
     * 
     * @param f The number of fractional seconds, min 0, max 9
     * @return The format pattern
     */
    public static String getFractionalSecondsTimestampFormat(int f) {
        String format = "yyyy-MM-dd HH:mm:ss";

        if (f > 9) {
            f = 9;
        }

        if (f > 0) {
            format = format + ".S";
            for (int i = 1; i < f; i++) {
                format = format + "S";
            }
        }

        return format;
    }
}
