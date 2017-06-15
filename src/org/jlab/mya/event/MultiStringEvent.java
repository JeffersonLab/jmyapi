package org.jlab.mya.event;

import java.time.Instant;
import org.jlab.mya.Event;
import org.jlab.mya.EventCode;

/**
 * Represents a Mya history event for a PV that is vector in nature or a PV that is not a scalar int
 * or float.
 *
 * @author slominskir
 */
public class MultiStringEvent extends Event {

    private final String[] value;

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
}
