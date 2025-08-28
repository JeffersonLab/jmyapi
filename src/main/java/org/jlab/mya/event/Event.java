package org.jlab.mya.event;

import java.time.Instant;
import org.jlab.mya.TimeUtil;

/**
 * A Mya history event such as an update or discontinuity marker.
 *
 * <p>This class is abstract as concrete implementations provide values of various data types which
 * may also be vector or scalar in nature. In practice Mya treats all vectors as collections of
 * Strings and only allows scalar floats and ints.
 *
 * @author slominskir
 */
public abstract class Event implements Comparable<Event> {

  /** The timestamp of the event. */
  private final long timestamp;

  /** The event code. */
  protected final EventCode code;

  /**
   * Create a new Event with the specified timestamp and event code.
   *
   * @param timestamp The Mya timestamp of the event
   * @param code The event code
   */
  protected Event(long timestamp, EventCode code) {
    this.timestamp = timestamp;
    this.code = code;
  }

  /**
   * Create a new Event with the specified timestamp and event code.
   *
   * @param timestamp The timestamp of the event
   * @param code The event code
   */
  protected Event(Instant timestamp, EventCode code) {
    this.timestamp = TimeUtil.toMyaTimestamp(timestamp);
    this.code = code;
  }

  /**
   * Deep Copy Event, but at a new instant in time.
   *
   * @param timeAsInstant The Instant
   * @return A new Event
   */
  public abstract Event copyTo(Instant timeAsInstant);

  /**
   * Return the timestamp.
   *
   * @return The timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Return the timestamp as the number of seconds from UNIX Epoch including fraction
   *
   * @return The number of seconds since UNIX Epoch including fractional part
   */
  public double getTimestampAsSeconds() {
    return timestamp * TimeUtil.MYATIME_TO_UNIX_SECONDS_WITH_FRACTION_SCALER;
  }

  /**
   * Return the timestamp as a Java Instant.
   *
   * @return The timestamp
   */
  public Instant getTimestampAsInstant() {
    return TimeUtil.fromMyaTimestamp(timestamp);
  }

  /**
   * Return the event code.
   *
   * @return The event code
   */
  public EventCode getCode() {
    return code;
  }

  /**
   * Compares Event objects according to their timestamps.
   *
   * @param e An Event object that is being compared
   * @return Negative if earlier, positive if later, zero if equal
   */
  @Override
  public int compareTo(Event e) {
    int cmp = 0;
    if (timestamp > e.getTimestamp()) {
      cmp = 1;
    } else if (timestamp < e.getTimestamp()) {
      cmp = -1;
    }
    return cmp;
  }
}
