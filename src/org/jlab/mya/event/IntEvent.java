package org.jlab.mya.event;

import java.time.Instant;
import org.jlab.mya.Event;
import org.jlab.mya.EventCode;

/**
 *
 * @author ryans
 */
public class IntEvent extends Event {
    private final int value;

    public IntEvent(Instant timestamp, EventCode code, int value) {
        super(timestamp, code);
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
