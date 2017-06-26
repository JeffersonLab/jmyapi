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

    @Override
    public String toString() {
        return toString(0);
    }

    public String toString(int f) {
        String format = "yyyy-MM-dd HH:mm:ss";

        if(f > 9) {
            f = 9;
        }
        
        if(f > 0) {
            format = format + ".S";
            for(int i = 1; i < f; i++) {
                format = format + "S";
            }
        }
        
        return timestamp.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(format))
                + " " + ((code == EventCode.UPDATE) ? String.valueOf(value) : "<"
                        + code.getDescription() + ">");
    }
}
