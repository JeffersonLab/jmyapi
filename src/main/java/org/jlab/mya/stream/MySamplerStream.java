package org.jlab.mya.stream;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.*;
import org.jlab.mya.nexus.DataNexus;

/**
 * This is a class mimics the command line mySampler application. It implements two different
 * strategies, sampling from the data as it is streamed through the library (STREAM), and repeatedly
 * querying the database for each sampled point (N_QUERIES). Different constructors create streams
 * that use either of these two strategy Previous versions attempted a hybrid approach, but that did
 * not work well since processing a ResultSet cannot be cleanly cancelled partway through.
 *
 * <p>For streams where the Event update rate is less than the requested sample rate, then it is
 * obviously better to make one query and stream all the underlying data. For streams where the
 * Event update rate is much, much, greater than the requested sample rate, making multiple queries
 * to the database is obviously better. Users should be aware of these trade-offs and select the
 * strategy they think best. Developer testing indicates the threshold for switching strategies to
 * maintain the best response time is somewhere around 5,000 events per sample.
 *
 * <p>Factory methods are provided to simplify the construction. Since this extends
 * BoundaryAwareStream, the factory methods are especially in dealing with the priorPoint which is
 * always required for MySamplerStream.
 *
 * @param <T> The Event type (FloatEvent, IntEvent, etc.)
 */
public class MySamplerStream<T extends Event> extends BoundaryAwareStream<T> {

  private final long intervalMillis;
  private final long sampleCount;
  private long samplesTaken = 0L;
  private long sampleTimeMya;
  private Instant sampleTimeInstant;
  private T currentEvent = null;
  private T previousEvent = null;
  private boolean firstRead = true;
  private boolean endOfStream = false;

  /** The different sampling strategies that can be used. */
  public enum Strategy {
    /**
     * Application-based sampling that is achieved by streaming data from the database as part of a
     * single query.
     */
    STREAM,
    /**
     * Sampling is done at the database level by making a SQL query for each individual sample that
     * is produced by the read() method.
     */
    N_QUERIES
  }

  private final Strategy strategy;
  private final DataNexus nexus;
  private final Metadata<T> metadata;

  /**
   * Create an instance of MySamplerStream that exclusively uses the N_QUERIES strategy. While this
   * class extends BoundaryAwareStream, this object makes no use of streaming the data, so no
   * wrapped stream or prior point are needed.
   *
   * @param begin The time of the first sample
   * @param intervalMillis The time interval between samples in milliseconds
   * @param sampleCount The number of samples to take, including the first one at begin
   * @param updatesOnly Should the final event from the BoundaryAwareStream only be an update event.
   * @param type The type of Event produced by the wrapped stream
   * @param nexus A DataNexus that can be used to perform the n-query strategy if needed. If null,
   *     only application sampling will be used.
   * @param metadata Metadata object for the wrapped EventStream. Used in the switch to n-query.
   */
  private MySamplerStream(
      Instant begin,
      long intervalMillis,
      long sampleCount,
      boolean updatesOnly,
      Class<T> type,
      DataNexus nexus,
      Metadata<T> metadata) {
    // In this case, we don't use the parent BoundaryAwareStream.  Setting the wrapped to null
    // should be harmless.
    super(
        null, begin, begin.plusMillis(intervalMillis * (sampleCount - 1)), null, updatesOnly, type);

    if (nexus == null || metadata == null) {
      throw new IllegalArgumentException(
          "Both nexus and metadata are required non-null the N_QUERIES strategy");
    }
    this.intervalMillis = intervalMillis;
    this.sampleCount = sampleCount;
    this.sampleTimeMya = TimeUtil.toMyaTimestamp(begin);
    this.sampleTimeInstant = begin;
    this.nexus = nexus;
    this.metadata = metadata;
    this.strategy = Strategy.N_QUERIES;
  }

  /**
   * Create an instance of MySamplerStream that exclusively uses the STREAM strategy. This implies
   * that the entire wrapped stream is processed and that the time required to perform the sampling
   * will be no faster than the time it takes to stream the data out of the database. This can be
   * faster than performing the N_QUERIES strategy if there are not many Events occurring between
   * sample points.
   *
   * @param wrapped A stream of Events to be sampled from
   * @param begin The time of the first sample
   * @param intervalMillis The time interval between samples in milliseconds
   * @param sampleCount The number of samples to take, including the first one at begin
   * @param priorPoint The Event prior to begin. It's required to be non-null.
   * @param updatesOnly Should the final event from the BoundaryAwareStream only be an update event.
   * @param type The type of Event produced by the wrapped stream
   */
  private MySamplerStream(
      EventStream<T> wrapped,
      Instant begin,
      long intervalMillis,
      long sampleCount,
      T priorPoint,
      boolean updatesOnly,
      Class<T> type) {
    super(
        wrapped,
        begin,
        begin.plusMillis(intervalMillis * (sampleCount - 1)),
        priorPoint,
        updatesOnly,
        type);
    this.intervalMillis = intervalMillis;
    this.sampleCount = sampleCount;
    this.sampleTimeMya = TimeUtil.toMyaTimestamp(begin);
    this.sampleTimeInstant = begin;

    // Since no nexus, metadata, or buffer information was passed, MySamplerStream will simply wrap
    // the underlying
    // stream and not try to do the n-query strategy.  No buffer needed.
    this.strategy = Strategy.STREAM;
    this.nexus = null;
    this.metadata = null;
  }

  /**
   * Generate an Event that represents a sample from a stream at a point where there is no data. For
   * example, sampling from before the start of the data or sampling from the future.
   *
   * @param timestamp The timestamp for which we want a sample event
   * @param type The type of point to generate
   * @param <T> The type of Event to generate
   * @return An event with EventCode.UNDEFINED and the desired timestamp
   */
  @SuppressWarnings("unchecked")
  private static <T extends Event> T generateMissingSampleEvent(Instant timestamp, Class<T> type) {
    T priorPoint;
    long priorTime = TimeUtil.toMyaTimestamp(timestamp);
    if (type == FloatEvent.class) {
      priorPoint = (T) new FloatEvent(priorTime, EventCode.UNDEFINED, 0f);
    } else if (type == IntEvent.class) {
      priorPoint = (T) new IntEvent(priorTime, EventCode.UNDEFINED, 0);
    } else if (type == MultiStringEvent.class) {
      priorPoint = (T) new MultiStringEvent(priorTime, EventCode.UNDEFINED, new String[] {});
    } else if (type == AnalyzedFloatEvent.class) {
      priorPoint = (T) new AnalyzedFloatEvent(priorTime, EventCode.UNDEFINED, 0f, new double[] {});
    } else if (type == LabeledEnumEvent.class) {
      priorPoint = (T) new LabeledEnumEvent(priorTime, EventCode.UNDEFINED, 0, "");
    } else {
      throw new IllegalArgumentException("Unsupported type " + type.getName());
    }
    return priorPoint;
  }

  /**
   * Factory method that produces a MySamplerStream that will only perform sampling at an
   * application level (i.e., STREAM strategy).
   *
   * @param wrapped The underlying EventStream to wrap
   * @param begin The time of the first sample point
   * @param intervalMillis The time between sample points
   * @param sampleCount The number of samples to return (including the first)
   * @param priorPoint The point to use as the first sampled value. If null, an Event with
   *     EventCode.UNDEFINED will be provided.
   * @param updatesOnly Should only non-disconnect events be processed
   * @param type The type of Event that is being streamed
   * @param <T> The Event type that is being streamed
   * @return A MySamplerStream of the wrapped EventStream
   */
  public static <T extends Event> MySamplerStream<T> getMySamplerStream(
      EventStream<T> wrapped,
      Instant begin,
      long intervalMillis,
      long sampleCount,
      T priorPoint,
      boolean updatesOnly,
      Class<T> type) {
    if (priorPoint == null) {
      priorPoint = generateMissingSampleEvent(begin.minusMillis(1), type);
    }
    return new MySamplerStream<>(
        wrapped, begin, intervalMillis, sampleCount, priorPoint, updatesOnly, type);
  }

  /**
   * Factory method that produces a MySamplerStream that exclusively performs the N_QUERIES
   * strategy. As such, there is no underlying EventStream or prior point to wrap.
   *
   * <p>Created largely for API symmetry as it's just a wrapper on the constructor
   *
   * @param begin The time of the first sample point
   * @param intervalMillis The time between sample points
   * @param sampleCount The number of samples to return (including the first)
   * @param updatesOnly Should only non-disconnect events be processed
   * @param type The type of Event that is being streamed
   * @param nexus A DataNexus that can be used to perform the n-query strategy if needed.
   * @param metadata Metadata object for the wrapped EventStream. Used in the switch to n-query.
   * @param <T> The type of Event that is to be streamed
   * @return A MySamplerStream of the wrapped EventStream
   */
  public static <T extends Event> MySamplerStream<T> getMySamplerStream(
      Instant begin,
      long intervalMillis,
      long sampleCount,
      boolean updatesOnly,
      Class<T> type,
      DataNexus nexus,
      Metadata<T> metadata) {
    return new MySamplerStream<>(
        begin, intervalMillis, sampleCount, updatesOnly, type, nexus, metadata);
  }

  /**
   * Read a sampled Event from the stream. This generates Events at the requested sample times that
   * have the same values and EventCodes as the nearest preceding Event from the underlying data.
   *
   * <p>If no preceding Event is found or if the sample time is from the future, then an Event with
   * EventCode.UNDEFINED is returned.
   *
   * <p>This method is capable of performing any of the supported sampling strategies. All should
   * return the same results.
   *
   * @return The next sampled event or null if end of stream is reached.
   * @throws IOException If trouble sapling data or an unsupported strategy is attempted.
   */
  public T read() throws IOException {
    try {
      // Have we got all of our samples?  Was there no data here to start?  Return null to indicate
      // nothing
      // left to read.
      if (endOfStream) {
        return null;
      }
      // We're trying to sample into the future.  Return a point with future timestamp and UNDEFINED
      // code.
      if (sampleTimeInstant.isAfter(now)) {
        T out = generateMissingSampleEvent(sampleTimeInstant, getType());
        moveSampleCursor();
        return out;
      }
      if (strategy == Strategy.STREAM) {
        return readStream();
      } else if (strategy == Strategy.N_QUERIES) {
        return readService();
      } else {
        throw new IOException("Unsupported sampling strategy");
      }
    } catch (SQLException ex) {
      throw new IOException("Error querying Database.", ex);
    }
  }

  /**
   * Read a single point from an underlying service (e.g., a PointService). This implements the
   * N_QUERIES strategy. Events with code UNDEFINED is returned when requesting a sample from the
   * future.
   *
   * @return A sampled Event
   * @throws SQLException If trouble querying database
   */
  @SuppressWarnings("unchecked")
  private T readService() throws SQLException {
    T event = nexus.findEvent(metadata, sampleTimeInstant, true, true, updatesOnly);
    T out;
    if (event == null) {
      out = generateMissingSampleEvent(sampleTimeInstant, getType());
    } else {
      out = (T) event.copyTo(sampleTimeInstant);
    }
    moveSampleCursor();
    return out;
  }

  /**
   * This does in app sampling of the full data stream. The basic idea is to return the value of the
   * signal only at the times that were requested based on sampling parameters. Events are created
   * so that timestamps are at sample times. Events with code UNDEFINED is returned when requesting
   * a sample from the future.
   *
   * <p>We maintained two event update points. When they straddle the sample time, we read out an
   * event with the sample time timestamp, but the value and code of the previous update event. Each
   * read from this stream will iterate over the wrapped stream until we find two points that
   * straddle the sample time or the end of the stream.
   *
   * @return The next sample event
   * @throws IOException If trouble with the stream
   * @throws SQLException If trouble querying data from the database
   */
  @SuppressWarnings("unchecked")
  public T readStream() throws IOException, SQLException {

    // On the first read, we need to get both current and previous to jump start the algorithm.
    // This should
    // skip through the later while loop to return the first point.
    if (firstRead) {
      firstRead = false;
      previousEvent = super.read();
      currentEvent = super.read();

      // No data found in the stream, mark the end of the stream.
      if (previousEvent == null) {
        endOfStream = true;
        return null;
      }
    }

    // If you've run past the final bounded value, add an event point at the next sample time.
    if (currentEvent == null) {
      //noinspection unchecked
      currentEvent = (T) previousEvent.copyTo(sampleTimeInstant);
    }

    // We need to work through the stream to find the next sample point.  We want the points
    // straddling the sample
    // time.
    while (determinePosition(previousEvent, currentEvent, sampleTimeMya) < 0) {
      previousEvent = currentEvent;
      currentEvent = super.read();

      // We've reached the end of the event stream.  Put currentEvent out to the next sample point,
      // so we now
      // straddle the current sample point on the next pass.
      if (currentEvent == null) {
        //noinspection SingleStatementInBlock,unchecked
        currentEvent = (T) previousEvent.copyTo(sampleTimeInstant.plusMillis(intervalMillis));
      }
    }

    // Take the sample
    //noinspection unchecked
    T out = (T) previousEvent.copyTo(sampleTimeInstant);
    moveSampleCursor();

    return out;
  }

  /**
   * Move the sample cursor (timestamp of next sample) forward by the requested interval. Track if
   * we have taken enough samples.
   */
  void moveSampleCursor() {
    sampleTimeInstant = sampleTimeInstant.plusMillis(intervalMillis);
    sampleTimeMya = TimeUtil.toMyaTimestamp(sampleTimeInstant);
    samplesTaken++;
    if (samplesTaken >= sampleCount) {
      endOfStream = true;
    }
  }

  /**
   * Determine the position of the two events relative to the desired sample time. Events e1 and e2
   * cannot be null. Required that e1.getTimestamp() <= e2.getTimestamp().
   *
   * @param e1 First event
   * @param e2 Second event
   * @param sample sample timestamp (MYA time format)
   * @return -1 if both are before, 1 if both are after, 0 if they straddle it
   */
  private int determinePosition(T e1, T e2, long sample) {
    if (e2.getTimestamp() < e1.getTimestamp()) {
      throw new IllegalArgumentException("e2 cannot be before e1.");
    }

    if (e1.getTimestamp() > sample) {
      return 1;
    } else if (e2.getTimestamp() <= sample) {
      return -1;
    } else {
      // e1 <= sample < e2
      return 0;
    }
  }

  /**
   * Get the current sampling strategy.
   *
   * @return The current sampling strategy.
   */
  public Strategy getStrategy() {
    return strategy;
  }

  /**
   * Close this stream and the stream it wraps if it exists.
   *
   * @throws IOException If trouble closing the stream
   */
  @Override
  public void close() throws IOException {
    if (wrapped != null) {
      wrapped.close();
    }
  }
}
