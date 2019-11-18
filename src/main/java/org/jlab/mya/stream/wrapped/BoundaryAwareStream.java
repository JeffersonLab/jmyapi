package org.jlab.mya.stream.wrapped;

import org.jlab.mya.Event;
import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.params.IntervalQueryParams;

import java.io.IOException;
import java.time.Instant;

/**
 * A BoundaryAwareStream will ensure an event (point) exists exactly on the beginning and end of an interval.
 * <p>
 * New points are added as needed (only if boundaries don't already contain points).   If needed, the prior point before
 * the interval is consulted to obtain the prior value and a new point is created with that value, but with the
 * beginning timestamp.  If needed a new point is created on the end boundary with the value of the last point
 * found in the interval.   Optionally the BoundaryAwareStream can attempt to set the end point to the last known
 * UPDATE event (thus avoiding non-update events), but this can result in no additional boundary point being added if
 * the stream contains only non-update events.
 * </p>
 *
 * @param <T>
 */
public class BoundaryAwareStream<T extends Event> extends WrappedEventStreamAdaptor<T, T>{

    private T lastEvent = null;
    private boolean started = false;
    private boolean buffered = false;
    private T priorPoint;
    private T secondPointBuffer;
    private final Instant begin;
    private final Instant end;
    private final boolean updatesOnly;

    /**
     * Create a new BoundaryAwareStream.
     *
     * @param wrapped The wrapped EventStream
     * @param begin The begin time
     * @param end The end time
     * @param priorPoint Prior point to copy to begin
     * @param updatesOnly Should end point be an update
     * @param type The type
     */
    public BoundaryAwareStream(EventStream<T> wrapped, Instant begin, Instant end, T priorPoint, boolean updatesOnly, Class<T> type) {
        super(wrapped, type);
        this.begin = begin;
        this.end = end;
        this.priorPoint = priorPoint;
        this.updatesOnly = updatesOnly;
    }

    /**
     * Read the next event from the stream. Generally you'll want to iterate
     * over the stream using a while loop.
     *
     * @return The next event or null if End-Of-Stream reached
     * @throws IOException If unable to read the next event
     */
    @Override
    @SuppressWarnings("unchecked")
    public T read() throws IOException {
        T current;

        if(started) {
            current = wrapped.read();
        } else {
           if(buffered) {
                current = secondPointBuffer;
                started = true;
            } else {
                current = wrapped.read();
                if (current != null && current.getTimestampAsInstant().isAfter(begin)) {
                    secondPointBuffer = current;
                    buffered = true;
                    current = (T) priorPoint.copyTo(begin);
                } else {
                    // current = current or current = null (end of stream / empty stream)
                    started = true;
                }
            }
        }

        if(current == null) { // End-of-Stream
            if(lastEvent != null && lastEvent.getTimestampAsInstant().isBefore(end)) {
                current = (T)lastEvent.copyTo(end);
                lastEvent = current;
            }
        } else {
            if(updatesOnly) {
                if(current.getCode() == EventCode.UPDATE) {
                    lastEvent = current;
                }
            } else {
                lastEvent = current;
            }
        }

        return current;
    }
}
