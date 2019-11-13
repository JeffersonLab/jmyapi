package org.jlab.mya.event;

import org.jlab.mya.EventCode;
import org.jlab.mya.TimeUtil;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class IntegratedFloatEvent extends FloatEvent {

    private final double integratedValue;

    /**
     * Create new FloatEvent.
     *
     * @param timestamp The Mya timestamp of the event
     * @param code The event code
     * @param value The event value
     */
    public IntegratedFloatEvent(long timestamp, EventCode code, float value, double integratedValue) {
        super(timestamp, code, value);
        this.integratedValue = integratedValue;
    }

    /**
     * Return the integrated value of the event.
     *
     * @return The value
     */
    public double getIntegratedValue() {
        return integratedValue;
    }
}

