package org.jlab.mya.event;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import org.jlab.mya.Event;
import org.jlab.mya.EventCode;
import org.jlab.mya.TimeUtil;

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
     * Factory method for constructing a MultiStringEvent from a row in a
     * database ResultSet.
     *
     * @param rs The ResultSet
     * @param size The size of the value vector (size 1 is acceptable)
     * @return A new MultiStringEvent
     * @throws SQLException If unable to create an Event from the ResultSet
     */
    public static MultiStringEvent fromRow(ResultSet rs, int size) throws SQLException {
        Instant timestamp = TimeUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.fromInt(codeOrdinal);
        String[] value = new String[size];
        int offset = 3;

        for (int i = 0; i < size; i++) {
            value[i] = rs.getString(i + offset);
        }
        return new MultiStringEvent(timestamp, code, value);
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
