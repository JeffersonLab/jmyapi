package org.jlab.mya.event;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.jlab.mya.EventCode;
import org.jlab.mya.TimeUtil;

/**
 *
 * @author ryans
 */
public class LabeledEnumEvent extends IntEvent {
    
    private final String label;
    
    public LabeledEnumEvent(Instant timestamp, EventCode code, int value, String label) {
        super(timestamp, code, value);
        
        this.label = label;
    }

    public String getLabel() {
        return label;
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
    @Override
    public String toString(int f) {
        String format = TimeUtil.getFractionalSecondsTimestampFormat(f);

        String result = timestamp.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(format))
                + " ";

        if (code == EventCode.UPDATE) {
            result = result + (label == null ? String.valueOf(value) : label);
        } else {
            result = result + "<" + code.getDescription() + ">";
        }

        return result;
    }      
}
