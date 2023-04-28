package org.jlab.mya.stream;

import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.Event;

import java.io.IOException;
import java.time.Instant;

/**
 * This is a class that attempts to mimic the commandline mySampler application.  You specify the start time, sample
 * interval in terms of time, and the number of samples, along with an EventStream and prior Event that contains the data
 * to be sampled from.
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

    /**
     * Create an instance of MySamplerStream.  This extends a special case of the BoundaryAwareStream that always has
     * priorPoint supplied.  The BoundaryAwareStream has 'begin' and 'end' values that match the first and last sample
     * needed in the mySamplerStream.
     *
     * @param wrapped        A stream of Events to be sampled from
     * @param begin          The time of the first sample
     * @param intervalMillis The time interval between samples in milliseconds
     * @param sampleCount    The number of samples to take, including the first one at begin
     * @param priorPoint     The Event prior to begin.  It's required to be non-null.
     * @param updatesOnly    Should the final event from the BoundaryAwareStream only be an update event.
     * @param type           The type of Event produced by the wrapped stream
     */
    public MySamplerStream(EventStream<T> wrapped, Instant begin, long intervalMillis, long sampleCount, T priorPoint,
                           boolean updatesOnly, Class<T> type) {
        super(wrapped, begin, begin.plusMillis(intervalMillis * (sampleCount - 1)), priorPoint, updatesOnly,
                type);
        if (priorPoint == null) {
            throw new IllegalArgumentException("mySamplerStream requires a non-null priorPoint.");
        }
        this.intervalMillis = intervalMillis;
        this.sampleCount = sampleCount;
        this.sampleTimeMya = TimeUtil.toMyaTimestamp(begin);
        this.sampleTimeInstant = begin;
    }

    /**
     * This does in app sampling of the full data stream.  The basic idea is to return the value of the signal only at
     * the times that were requested based on sampling parameters.  Events are created so that timestamps are at sample
     * times.  null is returned when requesting the future.
     * <p>
     * We maintained two event update points.  When they straddle the sample time, we read out an event with the sample
     * time timestamp, but the value and code of the previous update event.  Each read from this stream will iterate
     * over the wrapped stream until we find two points that straddle the sample time or the end of the stream.
     *
     * @return The next sample event or null if future.
     * @throws IOException If unable to read the next event
     */
    public T read() throws IOException {

        // Have we got all of our samples?  Was there no data here to start?  Return null to indicate there is nothing
        // left to read.
        if (endOfStream) {
            return null;
        }

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
            previousEvent = currentEvent;
            currentEvent = super.read();

            // We've reached the end of the event stream.  Put currentEvent out to the next sample point, so we now
            // straddle the current sample point.  This works for continued sampling into the future with only a single
            // pass through the loop.
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
     * @param sample sample timestamp
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
}