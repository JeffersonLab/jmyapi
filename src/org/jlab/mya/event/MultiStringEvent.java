package org.jlab.mya.event;

import java.time.Instant;
import org.jlab.mya.Event;
import org.jlab.mya.EventCode;

/**
 *
 * @author ryans
 */
public class MultiStringEvent extends Event {
    private final String[] value;

    public MultiStringEvent(Instant timestamp, EventCode code, String[] value) {
        super(timestamp, code);
        this.value = value;
    }

    public String[] getValue() {
        return value;
    }
}
