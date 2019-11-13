package org.jlab.mya.analysis;

/**
 * The stats for a single event (point).
 */
public class EventStats {
    private Double integration;
    private Double mean;

    /**
     * Create a new EventStats.
     *
     * @param integration The integration
     * @param mean The moving average
     */
    public EventStats(Double integration, Double mean) {
        this.integration = integration;
        this.mean = mean;
    }

    /**
     * Returns a weighted (numerically stable) integration.
     *
     * @return The integration
     */
    public Double getIntegration() {
        return integration;
    }

    /**
     * Returns a weighted cumulative moving average.
     *
     * @return The moving average
     */
    public Double getMovingAverage() {
        return mean;
    }

    public String toString() {
        return "Integration: " + integration + ", Moving Average: " + mean;
    }
}
