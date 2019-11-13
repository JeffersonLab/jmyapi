package org.jlab.mya.event;

import org.jlab.mya.EventCode;

public class AnalyzedFloatEvent extends FloatEvent {

    private final double[] stats;

    /**
     * Create new AnalyzedFloatEvent.
     *
     * @param timestamp The Mya timestamp of the event
     * @param code The event code
     * @param value The event value
     * @param stats The event stats
     */
    public AnalyzedFloatEvent(long timestamp, EventCode code, float value, double[] stats) {
        super(timestamp, code, value);
        this.stats = stats;
    }

    /**
     * Return the event stats.
     * <p>Stats are returns in a primitive double array for performance reasons.  The stats and their order is specified
     * in the constructor to the FloatAnalysisStream.</p>
     *
     * @return The stats
     */
    public double[] getEventStats() {
        return stats;
    }
}

