package org.jlab.mya;

import java.time.Instant;

/**
 *
 * @author ryans
 */
public abstract class Event {
    protected final Instant timestamp;
    protected final EventCode code;

    public Event(Instant timestamp, EventCode code) {
        this.timestamp = timestamp;
        this.code = code;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public EventCode getCode() {
        return code;
    }
}
