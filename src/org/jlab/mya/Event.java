package org.jlab.mya;

import java.time.Instant;

/**
 * A Mya history event such as an update or discontinuity marker.
 *
 * This class is abstract as concrete implementations provide values of various data types which may
 * also be vector or scalar in nature. In practice Mya treats all vectors as collections of Strings
 * and only allows scalar floats and ints.
 *
 * @author slominskir
 */
public abstract class Event implements Comparable<Event> {

    /**
     * The timestamp of the event.
     */
    protected final Instant timestamp;
    
    /**
     * The event code.
     */
    protected final EventCode code;

    /**
     * Create a new Event with the specified timestamp and event code.
     *
     * @param timestamp The timestamp of the event
     * @param code The event code
     */
    public Event(Instant timestamp, EventCode code) {
        this.timestamp = timestamp;
        this.code = code;
    }

    /**
     * Return the timestamp.
     *
     * @return The timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
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
     * Compares Event objects according to their Instant timestamps.
     * @param e An Event object that is being compared
     * @return Negative if earlier, Positive if later.
     */
    @Override
    public int compareTo(Event e) {
        return timestamp.compareTo(e.getTimestamp());
    }
}
