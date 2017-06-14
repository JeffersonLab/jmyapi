package org.jlab.mya.event;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.jlab.mya.Event;
import org.jlab.mya.EventCode;

/**
 *
 * @author ryans
 */
public class FloatEvent extends Event {
    private final float value;

    public FloatEvent(Instant timestamp, EventCode code, float value) {
        super(timestamp, code);
        this.value = value;
    }

    public float getValue() {
        return value;
    }

    @Override
    public String toString() {
        return timestamp.atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(
                "yyyy-MM-dd HH:mm:ss")) + " " + String.format("%32s", code) + " " + String.format(
                "%24s", value);
    }
}
