package org.jlab.mya.stream;

import org.jlab.mya.Metadata;
import org.jlab.mya.RingBuffer;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.*;
import org.jlab.mya.nexus.DataNexus;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

/**
 * This is a class mimics the command line mySampler application.  It implements two different strategies, sampling from
 * the data as it is streamed through the library (STREAM), and repeatedly querying the database for each sampled point
 * (N_QUERIES). Different constructors create streams that use either of these two strategy or a hybrid approach that
 * attempts to sampling from the data stream until the stream is observed to be too busy at which point the stream
 * switches to repeatedly querying the database.  Determining when to switch strategies is not universally obvious, but
 * this class uses default settings that should be a reasonable starting point.
 * <p>
 * For streams where the Event update rate is less than the requested sample rate, then it is obviously better to make
 * one query and stream all the underlying data.  For streams where the Event update rate is much, much, greater than
 * the requested sample rate, making multiple queries to the database is obviously better.  The setBuffer and
 * setStreamThreshold methods are provided to tune the switching behavior.  In most cases, changing the streamThreshold
 * (max number of events per sample) is all that should be needed.
 * <p>
 * Factory methods are provided to simplify the construction.  Since this extends BoundaryAwareStream, the factory
 * methods are especially in dealing with the priorPoint which is always required for MySamplerStream.
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
    private boolean strategyLocked;

    public enum Strategy {STREAM, N_QUERIES}

    private Strategy strategy;
    private final DataNexus nexus;
    private final Metadata<T> metadata;
    private RingBuffer buffer;
    private int bufferSize;
    private int bufferCheck;
    private long bufferCount;
    private int bufferDelay;
    private double streamThreshold;


    /**
     * Create an instance of MySamplerStream.  This extends a special case of the BoundaryAwareStream that always has
     * priorPoint supplied.  The BoundaryAwareStream has 'begin' and 'end' values that match the first and last sample
     * needed in the mySamplerStream.
     * <p>
     * This constructor creates a MySamplerStream that dynamically changes the sampling strategy based on the relative
     * update rates of the wrapped EventStream and the desired MySamplerStream.  MySamplerStream starts by performing
     * application-level sampling (STREAM strategy), meaning every Event from the wrapped EventStream is checked to see
     * if it is the basis for the next returned sample event.  Should the wrapped stream be estimated to have
     * streamThreshold more Events per sample point, the MySamplerStream will switch to N_QUERIES sampling strategy of
     * explicitly querying the database for each sample point.
     * <p>
     * This Stream periodically monitors the underlying EventStream to check its update rate. The first few points in
     * the MySamplerStream may be spaced unusually far apart, e.g. the first samples are from times before data was
     * available, so checking the stream is delayed by bufferDelay Events.  After this delay, the stream monitors
     * bufferSize Event updates after every bufferCheck updates.
     * <p>
     * Selecting optimal values for bufferSize, bufferCheck, bufferDelay, and streamThreshold depends on local operating
     * conditions.  This Constructor provides reasonable defaults for those values based on limited testing experience.
     * Testing was done absent connection pooling, but with database and client on same server.
     *
     * @param wrapped        A stream of Events to be sampled from
     * @param begin          The time of the first sample
     * @param intervalMillis The time interval between samples in milliseconds
     * @param sampleCount    The number of samples to take, including the first one at begin
     * @param priorPoint     The Event prior to begin.  It's required to be non-null.
     * @param updatesOnly    Should the final event from the BoundaryAwareStream only be an update event.
     * @param type           The type of Event produced by the wrapped stream
     * @param nexus          A DataNexus that can be used to perform the n-query strategy if needed.  If null, only
     *                       application sampling will be used.
     * @param metadata       Metadata object for the wrapped EventStream.  Used in the switch to n-query.
     *                       rates.
     */
    private MySamplerStream(EventStream<T> wrapped, Instant begin, long intervalMillis, long sampleCount, T priorPoint,
                            boolean updatesOnly, Class<T> type, DataNexus nexus, Metadata<T> metadata) {
        super(wrapped, begin, begin.plusMillis(intervalMillis * (sampleCount - 1)), priorPoint, updatesOnly,
                type);
        this.intervalMillis = intervalMillis;
        this.sampleCount = sampleCount;
        this.sampleTimeMya = TimeUtil.toMyaTimestamp(begin);
        this.sampleTimeInstant = begin;
        this.nexus = nexus;
        this.metadata = metadata;
        this.strategyLocked = false;
        this.strategy = Strategy.STREAM;

        // Set up the buffer behavior.
        setBuffer(50, 10_000, 50);
        // streamThreshold
        this.streamThreshold = 5_000;
    }


    /**
     * Create an instance of MySamplerStream that exclusively uses the N_QUERIES strategy.  While this class extends
     * BoundaryAwareStream, this object makes no use of streaming the data, so no wrapped stream or prior point are
     * needed.
     *
     * @param begin          The time of the first sample
     * @param intervalMillis The time interval between samples in milliseconds
     * @param sampleCount    The number of samples to take, including the first one at begin
     * @param updatesOnly    Should the final event from the BoundaryAwareStream only be an update event.
     * @param type           The type of Event produced by the wrapped stream
     * @param nexus          A DataNexus that can be used to perform the n-query strategy if needed.  If null, only
     *                       application sampling will be used.
     * @param metadata       Metadata object for the wrapped EventStream.  Used in the switch to n-query.
     */
    private MySamplerStream(Instant begin, long intervalMillis, long sampleCount, boolean updatesOnly, Class<T> type,
                            DataNexus nexus, Metadata<T> metadata) {
        // In this case, we don't use the parent BoundaryAwareStream.  Setting the wrapped to null should be harmless.
        super(null, begin, begin.plusMillis(intervalMillis * (sampleCount - 1)), null, updatesOnly,
                type);

        if (nexus == null || metadata == null) {
            throw new IllegalArgumentException("Both nexus and metadata are required non-null the N_QUERIES strategy");
        }
        this.intervalMillis = intervalMillis;
        this.sampleCount = sampleCount;
        this.sampleTimeMya = TimeUtil.toMyaTimestamp(begin);
        this.sampleTimeInstant = begin;
        this.nexus = nexus;
        this.metadata = metadata;
        this.strategyLocked = true;
        this.strategy = Strategy.N_QUERIES;

        // No stream to check, so set buffer to 0/null
        setBuffer(0, 0, 0);
        this.streamThreshold = 5_000;
    }

    /**
     * Create an instance of MySamplerStream that exclusively uses the STREAM strategy.  This implies that the
     * entire wrapped stream is processed and that the time required to perform the sampling will be no faster than the
     * time it takes to stream the data out of the database.  This can be faster than performing the N_QUERIES strategy
     * if there are not many Events occurring between sample points.
     *
     * @param wrapped        A stream of Events to be sampled from
     * @param begin          The time of the first sample
     * @param intervalMillis The time interval between samples in milliseconds
     * @param sampleCount    The number of samples to take, including the first one at begin
     * @param priorPoint     The Event prior to begin.  It's required to be non-null.
     * @param updatesOnly    Should the final event from the BoundaryAwareStream only be an update event.
     * @param type           The type of Event produced by the wrapped stream
     */
    private MySamplerStream(EventStream<T> wrapped, Instant begin, long intervalMillis, long sampleCount, T priorPoint,
                            boolean updatesOnly, Class<T> type) {
        super(wrapped, begin, begin.plusMillis(intervalMillis * (sampleCount - 1)), priorPoint, updatesOnly,
                type);
        this.intervalMillis = intervalMillis;
        this.sampleCount = sampleCount;
        this.sampleTimeMya = TimeUtil.toMyaTimestamp(begin);
        this.sampleTimeInstant = begin;

        // Since no nexus, metadata, or buffer information was passed, MySamplerStream will simply wrap the underlying
        // stream and not try to do the n-query strategy.  No buffer needed.
        this.strategy = Strategy.STREAM;
        this.strategyLocked = true;
        this.nexus = null;
        this.metadata = null;
        setBuffer(0, 0, 0);
        this.streamThreshold = 0;
    }

    /**
     * This is how many Event updates will be used to estimate stream update rate.
     *
     * @return The value of bufferSize.
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * This is how many Events should be streamed between making estimates of stream update rate.
     *
     * @return The value of bufferCheck
     */
    public int getBufferCheck() {
        return bufferCheck;
    }

    /**
     * This is how many events should be skipped at the beginning of the stream prior to estimating the stream update
     * rate.
     *
     * @return The value of bufferDelay
     */
    public int getBufferDelay() {
        return bufferDelay;
    }

    /**
     * This is how many Events have been read since the last trackBuffer call.
     *
     * @return The value of bufferCount
     */
    @SuppressWarnings("unused")
    public long getBufferCount() {
        return bufferCount;
    }

    /**
     * This is the maximum number of Events per requested sample before the STREAM strategy will be abandoned.
     *
     * @return The value of streamThreshold
     */
    @SuppressWarnings("unused")
    public double getStreamThreshold() {
        return streamThreshold;
    }

    /**
     * Set up the buffer and related parameters used to determine if a stream is too busy to continue using the STREAM
     * strategy.
     *
     * @param size  How many event intervals should be stored
     * @param check How often should the buffer be checked
     * @param delay How many events should be skipped before checking the stream at the start of reading from the stream
     */
    public void setBuffer(int size, int check, int delay) {
        if (size > check) {
            throw new IllegalArgumentException("Buffer size cannot be larger than check interval.");
        }
        if (delay > check - size) {
            throw new IllegalArgumentException("delay + size cannot be larger than the check interval");
        }
        bufferSize = size;
        bufferCheck = check;
        bufferDelay = delay;
        bufferCount = check - size - delay;
        if (bufferSize < 1) {
            buffer = null;
        } else {
            buffer = new RingBuffer(bufferSize);
        }
    }

    /**
     * This is the maximum number of Events per requested sample before the STREAM strategy will be abandoned.
     *
     * @param threshold the new threshold to be used
     */
    public void setStreamThreshold(double threshold) {
        streamThreshold = threshold;
    }

    /**
     * Generate an Event that represents a sample from a stream at a point where there is no data.  For example,
     * sampling from before the start of the data or sampling from the future.
     *
     * @param timestamp The timestamp for which we want a sample event
     * @param type      The type of point to generate
     * @param <T>       The type of Event to generate
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
            priorPoint = (T) new MultiStringEvent(priorTime, EventCode.UNDEFINED, new String[]{});
        } else if (type == AnalyzedFloatEvent.class) {
            priorPoint = (T) new AnalyzedFloatEvent(priorTime, EventCode.UNDEFINED, 0f, new double[]{});
        } else if (type == LabeledEnumEvent.class) {
            priorPoint = (T) new LabeledEnumEvent(priorTime, EventCode.UNDEFINED, 0, "");
        } else {
            throw new IllegalArgumentException("Unsupported type " + type.getName());
        }
        return priorPoint;
    }

    /**
     * Factory method that produces a MySamplerStream that will only perform sampling at an application level (i.e.,
     * STREAM strategy).
     *
     * @param wrapped        The underlying EventStream to wrap
     * @param begin          The time of the first sample point
     * @param intervalMillis The time between sample points
     * @param sampleCount    The number of samples to return (including the first)
     * @param priorPoint     The point to use as the first sampled value.  If null, an Event with EventCode.UNDEFINED
     *                       will be provided.
     * @param updatesOnly    Should only non-disconnect events be processed
     * @param type           The type of Event that is being streamed
     * @param <T>            The Event type that is being streamed
     * @return A MySamplerStream of the wrapped EventStream
     */

    public static <T extends Event> MySamplerStream<T> getMySamplerStream(EventStream<T> wrapped, Instant begin,
                                                                          long intervalMillis, long sampleCount,
                                                                          T priorPoint, boolean updatesOnly,
                                                                          Class<T> type) {
        if (priorPoint == null) {
            priorPoint = generateMissingSampleEvent(begin.minusMillis(1), type);
        }
        return new MySamplerStream<>(wrapped, begin, intervalMillis, sampleCount, priorPoint, updatesOnly, type);
    }

    /**
     * Factory method that produces a MySamplerStream that initially uses the STREAM strategy until the underlying
     * EventStream is observed to be too busy.  If the stream is too busy, then the strategy is switched to
     * N_QUERIES and locked.
     * <p>
     * See class documentation for more details.
     *
     * @param wrapped         The underlying EventStream to wrap
     * @param begin           The time of the first sample point
     * @param intervalMillis  The time between sample points
     * @param sampleCount     The number of samples to return (including the first)
     * @param priorPoint      The point to use as the first sampled value
     * @param updatesOnly     Should only non-disconnect events be processed
     * @param type            The type of Event that is being streamed
     * @param nexus           A DataNexus that can be used to perform the N_QUERIES strategy if needed.  If null, only
     *                        STREAM strategy will be used.
     * @param metadata        Metadata object for the wrapped EventStream.  Used in the switch to N_QUERIES.
     * @param bufferSize      How many event intervals should be tracked when determining if the EventStream is too
     *                        busy.  If null, use default value.
     * @param bufferCheck     How many events must pass before we check the if the stream is too busy.  If null, use
     *                        default value.
     * @param bufferDelay     How events do we skip before we start checking  if the stream is too busy.  If null, use
     *                        default value.
     * @param streamThreshold How much faster may the EventStream be than the desired sampled rate before switching
     *                        strategies.  Based on estimated rates.  If null, use default value.
     * @param <T>             The Event type that is being streamed
     * @return A MySamplerStream of the wrapped EventStream
     */
    public static <T extends Event> MySamplerStream<T> getMySamplerStream(EventStream<T> wrapped, Instant begin,
                                                                          long intervalMillis, long sampleCount,
                                                                          T priorPoint, boolean updatesOnly,
                                                                          Class<T> type, DataNexus nexus,
                                                                          Metadata<T> metadata, Integer bufferSize,
                                                                          Integer bufferCheck, Integer bufferDelay,
                                                                          Double streamThreshold) {
        if (priorPoint == null) {
            priorPoint = generateMissingSampleEvent(begin.minusMillis(1), type);
        }
        MySamplerStream<T> stream = new MySamplerStream<>(wrapped, begin, intervalMillis, sampleCount, priorPoint,
                updatesOnly, type, nexus, metadata);
        if (streamThreshold != null) {
            stream.setStreamThreshold(streamThreshold);
        }
        if (bufferSize != null || bufferCheck != null || bufferDelay != null) {
            int size = (bufferSize == null) ? stream.getBufferSize() : bufferSize;
            int check = (bufferCheck == null) ? stream.getBufferCheck() : bufferCheck;
            int delay = (bufferDelay == null) ? stream.getBufferDelay() : bufferDelay;
            stream.setBuffer(size, check, delay);
        }
        return stream;
    }

    /**
     * Factory method that produces a MySamplerStream that exclusively performs the N_QUERIES strategy.  As such, there
     * is no underlying EventStream or prior point to wrap.
     *
     * @param begin          The time of the first sample point
     * @param intervalMillis The time between sample points
     * @param sampleCount    The number of samples to return (including the first)
     * @param updatesOnly    Should only non-disconnect events be processed
     * @param type           The type of Event that is being streamed
     * @param nexus          A DataNexus that can be used to perform the n-query strategy if needed.
     * @param metadata       Metadata object for the wrapped EventStream.  Used in the switch to n-query.
     * @return A MySamplerStream of the wrapped EventStream
     * @apiNote Created largely for API symmetry as it's just a wrapper on the constructor
     */
    public static <T extends Event> MySamplerStream<T> getMySamplerStream(Instant begin, long intervalMillis,
                                                                          long sampleCount, boolean updatesOnly,
                                                                          Class<T> type, DataNexus nexus,
                                                                          Metadata<T> metadata) {
        return new MySamplerStream<>(begin, intervalMillis, sampleCount, updatesOnly, type, nexus, metadata);
    }

    /**
     * Read a sampled Event from the stream.  This generates Events at the requested sample times that have the same
     * values and EventCodes as the nearest preceding Event from the underlying data.
     * <p>
     * If no preceding Event is found or if the sample time is from the future, then an Event with EventCode.UNDEFINED
     * is returned.
     * <p>
     * This method is capable of performing any of the supported sampling strategies.  All should return the same
     * results.
     *
     * @return The next sampled event or null if end of stream is reached.
     * @throws IOException If trouble sapling data or an unsupported strategy is attempted.
     */
    public T read() throws IOException {
        try {
            // Have we got all of our samples?  Was there no data here to start?  Return null to indicate nothing
            // left to read.
            if (endOfStream) {
                return null;
            }
            // We're trying to sample into the future.  Return a point with future timestamp and UNDEFINED code.
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
     * Read a single point from an underlying service (e.g., a PointService).  This implements the N_QUERIES strategy.
     * Events with code UNDEFINED is returned when requesting a sample from the future.
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
     * This does in app sampling of the full data stream.  The basic idea is to return the value of the signal only at
     * the times that were requested based on sampling parameters.  Events are created so that timestamps are at sample
     * times.  Events with code UNDEFINED is returned when requesting  a sample from the future.
     * <p>
     * We maintained two event update points.  When they straddle the sample time, we read out an event with the sample
     * time timestamp, but the value and code of the previous update event.  Each read from this stream will iterate
     * over the wrapped stream until we find two points that straddle the sample time or the end of the stream.
     *
     * @return The next sample event
     * @throws IOException If unable to read the next event
     */
    @SuppressWarnings("unchecked")
    public T readStream() throws IOException, SQLException {

        // On the first read, we need to get both current and previous to jump start the algorithm.  This should
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

        // We need to work through the stream to find the next sample point.  We want the points straddling the sample
        // time.
        while (determinePosition(previousEvent, currentEvent, sampleTimeMya) < 0) {
            trackBuffer();
            if (strategy == Strategy.N_QUERIES) {
                wrapped.close();
                return readService();
            }
            previousEvent = currentEvent;
            currentEvent = super.read();

            // We've reached the end of the event stream.  Put currentEvent out to the next sample point, so we now
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
     * Update the event interval buffer if conditions are met.  Do nothing if we have no buffer, we are no longer
     * open to changing strategies, are not using the STREAM strategy, or if we aren't within bufferSize Event of
     * the next bufferCheck.
     * <p>
     * If the buffer suggests the stream is too busy to continue reading from, then we switch to the N_QUERIES strategy
     * and lock the strategy to indicate that we should not switch back.
     * <p>
     */
    void trackBuffer() {
        // Skip this if we don't have a buffer.  Or skip this if we have locked into a certain strategy.
        if (buffer == null || strategyLocked) {
            return;
        }

        if (strategy == Strategy.STREAM && bufferCount >= (bufferCheck - bufferSize)) {
            buffer.write(currentEvent.getTimestampAsInstant().toEpochMilli() -
                    previousEvent.getTimestampAsInstant().toEpochMilli());
            bufferCount++;
            if (bufferCount == bufferCheck) {
                bufferCount = 0;
                // This compares the interval between event arrivals to the sampling interval
                // (smaller interval == faster rate).
                if (buffer.getMean() * streamThreshold < intervalMillis) {
                    strategy = Strategy.N_QUERIES;
                    strategyLocked = true;
                }
            }
        } else {
            bufferCount++;
        }
    }

    /**
     * Move the sample cursor (timestamp of next sample) forward by the requested interval.  Track if we have taken
     * enough samples.
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
     * Determine the position of the two events relative to the desired sample time.  Events e1 and e2 cannot be null.
     * Required that e1.getTimestamp() <= e2.getTimestamp().
     *
     * @param e1     First event
     * @param e2     Second event
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
     * Check if the strategy is locked.
     *
     * @return If we allow any changes to the sampling strategy
     */
    @SuppressWarnings("unused")
    public boolean isStrategyLocked() {
        return strategyLocked;
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