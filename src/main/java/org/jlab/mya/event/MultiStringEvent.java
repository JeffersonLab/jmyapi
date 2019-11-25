package org.jlab.mya.event;

import java.time.Instant;

/**
 * Represents a Mya history event for a PV that is vector in nature (regardless
 * of data type) or a PV that is not a scalar int or float.
 *
 * @author slominskir
 */
public class MultiStringEvent extends Event {

    private final String[] value;

    /**
     * Create new MultiStringEvent.
     *
     * @param timestamp The Mya timestamp of the event
     * @param code The event code
     * @param value The event value
     */
    public MultiStringEvent(long timestamp, EventCode code, String[] value) {
        super(timestamp, code);
        this.value = value;
    }

    /**
     * Create new MultiStringEvent.
     *
     * @param timestamp The timestamp of the event
     * @param code The event code
     * @param value The event value
     */
    public MultiStringEvent(Instant timestamp, EventCode code, String[] value) {
        super(timestamp, code);
        this.value = value;
    }

    /**
     * Return the value.
     *
     * @return The value
     */
    public String[] getValue() {
        return value;
    }

    /**
     * Deep Copy Event, but at a new instant in time.
     *
     * @return A new Event
     */
    @Override
    public MultiStringEvent copyTo(Instant timeAsInstant) {
        return new MultiStringEvent(timeAsInstant, this.getCode(), this.getValue());
    }
}
