package org.jlab.mya;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Utility methods for time including conversion between Mya's database time format and Java's
 * native Instant.
 *
 * <p>Mya stores each timestamp in the database as a 64-bit field where the top half of the field is
 * UNIX time and the bottom half is fractional seconds.
 *
 * <p>The fractional seconds are not something straight forward like the number of nano-seconds.
 * Instead they are treated similar to how NTP handles fractional seconds: 1 second = 1 / 2^32
 * fractional seconds.
 *
 * <p>Fractional seconds (floating point) stored in 32 bits cannot always be converted to
 * nanoseconds (integer) and back again to fractional seconds such that the number you start with is
 * the same as the one you end with. The C++ API uses a max precision of microseconds, but even then
 * may not guarantee invertability.
 *
 * @author slominskir, adamc
 */
public final class TimeUtil {

  /**
   * To seconds from a myatime timestamp. Similar to the nanosecond scaling factor times 10e-9 which
   * scales from nanoseconds to seconds. Will include fractional seconds.
   */
  public static final double MYATIME_TO_UNIX_SECONDS_WITH_FRACTION_SCALER = 3.725290298461914e-9;

  /**
   * To Nanoseconds scaling factor.
   *
   * <p>3.725290298461914 (3.7252902984619140625) {10^9/2^28} is a scaling factor to convert from
   * 2^28 fractional seconds to nanoseconds
   */
  private static final double TO_NANO_SCALER = 3.725290298461914;

  /**
   * From Nanoseconds scaling factor.
   *
   * <p>0.268435456 = 1 / 3.725290298461914 (scaling factor to convert from nanoseconds to 2^28
   * fractional seconds)
   */
  private static final double FROM_NANO_SCALER = 0.268435456;

  /** Bit mask for clearing high 36-bits of 64-bit long leaving only low 28-bits. */
  private static final long HI_MASK = (1L << 28) - 1;

  /** Instantiating one of these is useless so don't do it. */
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
    long hi; // Integer to fill upper bits

    // Given that myatime is a 36 bit unixtime (seconds since epoch) this conversion will overflow
    // thousands of years into the future.
    hi = instant.getEpochSecond();
    lo = instant.getNano();

    // Java has no unsigned types, but we can use a long to hold the value of an unsigned integer
    long unsignedLo = Integer.toUnsignedLong(lo);
    unsignedLo = (long) (unsignedLo * FROM_NANO_SCALER);
    lo = (int) unsignedLo;

    // MYA time has two components in a single 64 bit long.  The upper 36 are an extended Unix time
    // (seconds since epoch) and
    // the lower 28 bits is the fractional second component.  Shift the hi part over 28 bits and
    // keep only the lower 28 of the 32 bit
    // integer holding the count of 2^-28 seconds
    return (hi << 28) | (lo & 0xfffffffL);
  }

  /**
   * Convert a Mya timestamp to a java.time.Instant.
   *
   * @param timestamp The Mya timestamp
   * @return The Instant
   */
  public static Instant fromMyaTimestamp(long timestamp) {
    int lo;
    long hi;

    hi = timestamp >>> 28; // >> 28 means sign extend; >>> 28 means zero-fill...

    // Java has no unsigned types, but we can use a long to hold the value of an unsigned integer.
    // This is the number
    // of 2^-28 seconds.
    long unsignedLo = timestamp & HI_MASK;

    // Now the number of nanoseconds should hopefully be no more than 30 bits of real data (28 + 2).
    //  Implicit double conversion
    // will result in at most a 30 bit integer component, and the fraction piece will get truncated
    // on cast to long.
    unsignedLo = (long) (unsignedLo * TO_NANO_SCALER);

    // Conversion to int is safe since we have at most 30 bits, and int has 31 bits for unsigned
    // data
    lo = (int) unsignedLo;

    return Instant.ofEpochSecond(hi, lo);
  }

  /**
   * Obtain a DateTimeFormatter pattern string with the specified number of fractional seconds.
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

  /**
   * Returns an Instant in the local timezone given the specified timezone-less date and time
   * formatted as an ISO 8601 String.
   *
   * @param iso8601Timestamp The timestamp String
   * @return The Instant
   */
  public static Instant toLocalDT(String iso8601Timestamp) {
    return LocalDateTime.parse(iso8601Timestamp).atZone(ZoneId.systemDefault()).toInstant();
  }
}
