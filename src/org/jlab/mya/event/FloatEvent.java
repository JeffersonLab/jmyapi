package org.jlab.mya.event;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.jlab.mya.Event;
import org.jlab.mya.EventCode;

/**
 * Represents a Mya history event for a PV of data type float.
 * 
 * @author slominskir
 */
public class FloatEvent extends Event {
    private final float value;

    /**
     * Create new FloatEvent.
     * 
     * @param timestamp The timestamp of the event
     * @param code The event code
     * @param value The event value
     */
    public FloatEvent(Instant timestamp, EventCode code, float value) {
        super(timestamp, code);
        this.value = value;
    }

    /**
     * Return the value of the event.
     * 
     * @return The value
     */
    public float getValue() {
        return value;
    }

    @Override
    public String toString() {
        return timestamp.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(
                "yyyy-MM-dd HH:mm:ss")) + " " + ((code == EventCode.UPDATE) ? String.valueOf(value) : "<" + code.getDescription() + ">");
    }
}
