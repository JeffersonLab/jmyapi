package org.jlab.mya.event;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.jlab.mya.Event;
import org.jlab.mya.EventCode;
import org.jlab.mya.TimeUtil;

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
     * @param timestamp The Mya timestamp of the event
     * @param code The event code
     * @param value The event value
     */
    public FloatEvent(long timestamp, EventCode code, float value) {
        super(timestamp, code);
        this.value = value;
    }

    /**
     * Create new FloatEvent.
     *
     * @param timestamp The Mya timestamp of the event
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

    /**
     * Return a String representation of this FloatEvent using a timestamp with zero fractional
     * seconds displayed.
     *
     * @return A String representation
     */
    @Override
    public String toString() {
        return toString(0);
    }

    /**
     * Return a String representation of this FloatEvent using a timestamp with the specified
     * fractional second precision displayed. Note: using a value of 6 (microseconds) is generally
     * the max precision (A precision up to 9, nanoseconds, is supported, but rounding errors
     * prevent proper conversion).
     *
     * @param f The fractional seconds (-f in myget)
     * @return The String representation
     */
    public String toString(int f) {
        String format = TimeUtil.getFractionalSecondsTimestampFormat(f);

        return this.getTimestampAsInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(format))
                + " " + ((code == EventCode.UPDATE) ? String.valueOf(value) : "<"
                        + code.getDescription() + ">");
    }

    /**
     * Deep Copy Event, but at a new instant in time.
     *
     * @return A new Event
     */
    public FloatEvent copyTo(Instant timeAsInstant) {
        return new FloatEvent(timeAsInstant, this.getCode(), this.getValue());
    }
}
