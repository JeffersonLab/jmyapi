package org.jlab.mya.event;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.jlab.mya.TimeUtil;

/**
 * Represents a Mya history event for a PV of data type int.
 *
 * @author slominskir
 */
public class IntEvent extends Event {

    /**
     *MYA channel value
     */
    final int value;

    /**
     * Create new IntEvent.
     *
     * @param timestamp The Mya timestamp of the event
     * @param code The event code
     * @param value The event value
     */
    public IntEvent(long timestamp, EventCode code, int value) {
        super(timestamp, code);
        this.value = value;
    }

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

    /**
     * Return a String representation of this IntEvent using a timestamp with
     * zero fractional seconds displayed.
     *
     * @return A String representation
     */
    @Override
    public String toString() {
        return toString(0);
    }

    /**
     * Return a String representation of this IntEvent using a timestamp with
     * the specified fractional second precision displayed. Note: using a value
     * of 6 (microseconds) is generally the max precision (A precision up to 9,
     * nanoseconds, is supported, but rounding errors prevent proper
     * conversion).
     *
     * @param f The fractional seconds (-f in myget)
     * @return The String representation
     */
    public String toString(int f) {
        String format = TimeUtil.getFractionalSecondsTimestampFormat(f);

        String result = this.getTimestampAsInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(format))
                + " ";

        if (code.isDisconnection()) {
            result = result + "<" + code.getDescription() + ">";
        } else {
            result = result + value;
            if (!code.getDescription().isEmpty()) {
                result = result + " <" + code.getDescription() + ">";
            }
        }

        return result;
    }

    /**
     * Deep Copy Event, but at a new instant in time.
     *
     * @return A new Event
     */
    @Override
    public IntEvent copyTo(Instant timeAsInstant) {
        return new IntEvent(timeAsInstant, this.getCode(), this.getValue());
    }
}
