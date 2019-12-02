package org.jlab.mya.stream;

import org.jlab.mya.event.Event;
import org.jlab.mya.event.EventCode;

import java.io.IOException;
import java.time.Instant;

/**
 * A BoundaryAwareStream will attempt to provide an event (point) exactly on the beginning and end of an interval.
 * <p>
 * New points are added only if boundaries don't already contain points and only if values can be determined.
 * The prior point before the interval is consulted, if available, to obtain the prior value and a new point can be
 * created with the prior point value, but with the beginning boundary timestamp.  A new point is created on the end
 * boundary with the value of the last point found in the interval, but only if a point does not already exist on the
 * boundary and only if the boundary is not in the future.   Optionally the BoundaryAwareStream can attempt to set the
 * end point to the last known UPDATE event (thus avoiding non-update events), but this can result in no
 * boundary point being added if the stream contains only non-update events.
 * </p>
 *
 * @param <T> The type
 */
public class BoundaryAwareStream<T extends Event> extends WrappedStream<T, T> {

    private T lastEvent = null;
    private boolean started = false;
    private boolean buffered = false;
    private final T priorPoint;
    private T secondPointBuffer;
    private final Instant begin;
    private final Instant end;
    private final boolean updatesOnly;
    private final Instant now;

    /**
     * Create a new BoundaryAwareStream.
     *
     * @param wrapped The wrapped EventStream
     * @param begin The begin time
     * @param end The end time
     * @param priorPoint Prior point to copy to begin or null if unavailable
     * @param updatesOnly Should end point be an update
     * @param type The type
     */
    public BoundaryAwareStream(EventStream<T> wrapped, Instant begin, Instant end, T priorPoint, boolean updatesOnly, Class<T> type) {
        super(wrapped, type);
        this.begin = begin;
        this.end = end;
        this.priorPoint = priorPoint;
        this.updatesOnly = updatesOnly;
        this.now = Instant.now(); /* We consider "now" to be fixed in time from when stream created */
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
                if (current != null // Not End-of-Stream
                        && current.getTimestampAsInstant().isAfter(begin) // First event after start boundary
                        && priorPoint != null) { // Prior point provided / exists
                    secondPointBuffer = current;
                    buffered = true;
                    current = (T) priorPoint.copyTo(begin);
                } else {
                    // Don't insert prior point; just move on
                    started = true;
                }
            }
        }

        if(current == null) { // End-of-Stream
            if(lastEvent != null // Last event exists
                    && lastEvent.getTimestampAsInstant().isBefore(end) // Last event before end boundary
                    && end.isBefore(now)) { // end boundary is before "now"
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
