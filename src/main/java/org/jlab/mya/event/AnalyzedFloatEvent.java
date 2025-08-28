package org.jlab.mya.event;

import java.time.Instant;
import org.jlab.mya.TimeUtil;

/**
 * Represents a Mya history event for a PV of data type float, and includes extra analysis
 * statistics.
 */
public class AnalyzedFloatEvent extends FloatEvent {

  private final double[] stats;

  /**
   * Create new AnalyzedFloatEvent.
   *
   * @param timestamp The Mya timestamp of the event
   * @param code The event code
   * @param value The event value
   * @param stats The event stats
   */
  public AnalyzedFloatEvent(long timestamp, EventCode code, float value, double[] stats) {
    super(timestamp, code, value);
    this.stats = stats;
  }

  /**
   * Return the event stats.
   *
   * <p>Stats are returns in a primitive double array for performance reasons. The stats and their
   * order is specified in the constructor to the FloatAnalysisStream responsible for generating
   * them.
   *
   * @return The stats
   */
  public double[] getEventStats() {
    return stats;
  }

  /**
   * Deep Copy Event, but at a new instant in time.
   *
   * @return A new Event
   */
  @Override
  public AnalyzedFloatEvent copyTo(Instant timeAsInstant) {
    return new AnalyzedFloatEvent(
        TimeUtil.toMyaTimestamp(timeAsInstant),
        this.getCode(),
        this.getValue(),
        this.getEventStats());
  }
}
