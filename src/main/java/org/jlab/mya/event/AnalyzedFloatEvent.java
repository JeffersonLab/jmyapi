package org.jlab.mya.event;

import org.jlab.mya.EventCode;
import org.jlab.mya.analysis.EventStats;

public class AnalyzedFloatEvent extends FloatEvent {

    private final EventStats stats;

    /**
     * Create new AnalyzedFloatEvent.
     *
     * @param timestamp The Mya timestamp of the event
     * @param code The event code
     * @param value The event value
     * @param stats The event stats
     */
    public AnalyzedFloatEvent(long timestamp, EventCode code, float value, EventStats stats) {
        super(timestamp, code, value);
        this.stats = stats;
    }

    /**
     * Return the event stats.
     *
     * @return The stats
     */
    public EventStats getEventStats() {
        return stats;
    }
}

