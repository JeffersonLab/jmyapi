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
     * Factory method for constructing an IntEvent from a row in a database
     * ResultSet.
     *
     * @param rs The ResultSet
     * @return A new IntEvent
     * @throws SQLException If unable to create an Event from the ResultSet
     */
    public static IntEvent fromRow(ResultSet rs) throws SQLException {
        Instant timestamp = TimeUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.fromInt(codeOrdinal);
        int value = rs.getInt(3);
        return new IntEvent(timestamp, code, value);
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
        return toString(0, null);
    }

    /**
     * Return a String representation of this IntEvent using a timestamp with
     * the specified fractional second precision displayed. Note: using a value
     * of 6 (microseconds) is generally the max precision (A precision up to 9,
     * nanoseconds, is supported, but rounding errors prevent proper
     * conversion).
     *
     * @param f The fractional seconds (-f in myget)
     * @param enumLabels Optional enum labels to use, null if not needed
     * @return The String representation
     */
    public String toString(int f, String[] enumLabels) {
        String format = TimeUtil.getFractionalSecondsTimestampFormat(f);

        String result = timestamp.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(format))
                + " ";

        if (code == EventCode.UPDATE) {
            if (enumLabels != null && enumLabels.length > value) {
                result = result + enumLabels[value];
            } else {
                result = result + String.valueOf(value);
            }
        } else {
            result = result + "<" + code.getDescription() + ">";
        }

        return result;
    }
}
