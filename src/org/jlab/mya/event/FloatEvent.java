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
     * @param timestamp The timestamp of the event
     * @param code The event code
     * @param value The event value
     */
    public FloatEvent(Instant timestamp, EventCode code, float value) {
        super(timestamp, code);
        this.value = value;
    }

    /**
     * Factory method for constructing a FloatEvent from a row in a database ResultSet.
     *
     * @param rs The ResultSet
     * @return A new FloatEvent
     * @throws SQLException If unable to create an Event from the ResultSet
     */
    public static FloatEvent fromRow(ResultSet rs) throws SQLException {
        Instant timestamp = TimeUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.fromInt(codeOrdinal);
        float value = rs.getFloat(3);
        return new FloatEvent(timestamp, code, value);
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
        String format = "yyyy-MM-dd HH:mm:ss";

        if (f > 9) {
            f = 9;
        }

        if (f > 0) {
            format = format + ".S";
            for (int i = 1; i < f; i++) {
                format = format + "S";
            }
        }

        return timestamp.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(format))
                + " " + ((code == EventCode.UPDATE) ? String.valueOf(value) : "<"
                        + code.getDescription() + ">");
    }
}
