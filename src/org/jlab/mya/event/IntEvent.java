package org.jlab.mya.event;

import java.time.Instant;
import org.jlab.mya.Event;
import org.jlab.mya.EventCode;

/**
 * Represents a Mya history event for a PV of data type int.
 * 
 * @author slominskir
 */
public class IntEvent extends Event {
    private final int value;

    /**
     * Create new IntEvent.
     * 
     * @param timestamp The timestamp of the event
     * @param code The event code
     * @param value The event value
     */    
    public IntEvent(Instant timestamp, EventCode code, int value) {
        super(timestamp, code);
        this.value = value;
    }

    /**
     * Return the value.
     * 
     * @return The value
     */
    public int getValue() {
        return value;
    }
}
