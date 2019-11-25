package org.jlab.mya;

import org.jlab.mya.event.EventCode;
import org.jlab.mya.event.FloatEvent;

/**
 * This class provides summary statistics about the specified set of MYA data.  This class makes a best effort at using
 * online and numerically stable algorithms for calculating the provided statistics.
 * <p>
 * The class can be used to obtain stats on the entire series, but also incrementally per event.
 * </p>
 * @author apcarp
 * @author slominskir
 */
public class RunningStatistics {

    // Set to true once the first Update event has been push'ed
    private boolean initialized = false;

    // These will be null until sufficient data is available for the statistic to be valid.
    private FloatEvent prev = null;
    private FloatEvent curr = null;
    // Primitives initialize to zero by default
    private double min;
    private double max;
    private double mean;
    private double sigmaSum;
    private double duration; // Stored in seconds

    private double integration; // PV Units * seconds
    private double correction; // Correction parameter used in calculating the integration

    private long eventCount;
    private long updateCount;

    public static final short INTEGRATION = 0;

    private final short[] eventStatsMap;

    /**
     * Create an instance of the RunningStatistics class
     * that tracks series stats, but no event stats.
     */
    public RunningStatistics() {
        this(new short[0]);
    }

    /**
     * Create an instance of the RunningStatistics class
     * that tracks series stats, plus the specified event stats.
     * <p>
     * Event Stats are specified by short value and array index.  Valid values
     * are assigned constants and include RunningStatistics.INTEGRATION.
     * </p>
     * @param eventStatsMap The event stats to track
     */
    public RunningStatistics(short[] eventStatsMap) {
        if(eventStatsMap == null) {
            this.eventStatsMap = new short[0];
        } else {
            this.eventStatsMap = eventStatsMap;
        }
    }

    // Useful as a shorthand  for "initializing" an assortment of statistics
    private void zeroNums() {
        min = max = mean = sigmaSum = duration = integration = 0; // TODO: should correction be reset to zero here too?
    }

    // All currently calculated statistics will be valid once we have processed the first two events.  prev is set to something not
    // null when this happens.
    private boolean statsValid() {
        return (prev != null);
    }

    /**
     * This method clears the running statistics calculated thus far, resetting
     * the RunningStatistics object to it's initial state.
     */
    public void reset() {
        initialized = false;
        zeroNums();
        prev = curr = null;
    }

    /**
     * This function updates the set of running statistics for a given channel
     * history for the given event.
     *
     * @param event The next event in the channel history to add to the
     * calculation of statistics.
     */
    public void push(FloatEvent event) {
        
        eventCount++;

        // The first event just gets saved.  Every duration calculation requires two events.
        if (curr == null) {
            curr = event;
            return;
        }
        // Grab the next event
        prev = curr;
        curr = event;

        // Statistics are only valid for UPDATE and should be weighted/normalized over the time that we were in an UPDATE state
        if (prev.getCode().equals(EventCode.UPDATE)) {
            updateCount++;
            double value = prev.getValue();

            // Convert weight to seconds - helps both conceptually and with rounding errors (seconds will be more
            // central than nanos or millis, which should on average yield more consistent scales for operations).

            //System.out.println("curr: " + curr + ", prev: " + prev);

            double weight = curr.getTimestampAsSeconds() - prev.getTimestampAsSeconds();
            updateStatistics(value, weight);
        }
    }

    /**
     * Mean and variance are calculated using a modified version of a
     * numerically stable one pass algorithm presented in "Incremental
     * calculation of weighted mean and variance" by Tony Finch, University of
     * Cambridge Computing Service, Feb 2009.
     *
     * The time integral of the MYA channel history is also calculated. Time
     * domains without valid data are excluded, and due to the piece-wise
     * constant nature of channel histories, the integral calculation is
     * simplified to a time-weighted sum of the values of the channel history
     * during the requested time period. This summation is performed using a
     * Kahan/Neumaier summation algorithm.
     *
     * Note: Events must be added in the chronological order they occurred and
     * must include non Update events in order for the statistics be valid.
     *
     * Aside: The implementation for variance (sigma) calculation closely
     * follows the algorithm in the C++ myapi. However, while that version
     * specifies using this one pass algorithm for numerically stability and
     * that API documentation makes a reference to using the "'Discrete Data
     * Set' computation as referred to in mathematics", it is not explicitly
     * stated why this is this correct algorithm for computing variance of a
     * continuous time-domain function. I have created sketch of a proof as to
     * why this is a reasonable way to calculate the variance of a channel
     * history and will supply in GitHub project documents.
     *
     * @param value The value of the event to be added
     * @param weight The weight to be associated with the value. Typically the
     * time duration of the value.
     */
    private void updateStatistics(double value, double weight) {
        duration = duration + weight;  // Total duration of UPDATE Events so far
        // Note: duration initialized to zero by default java constructor behavior
        if (!initialized) {
            initialized = true;
            min = max = mean = value;
            integration = value * weight;
            // Note: sigmaSum initialized to zero by default java constructor behavior

        } else {
            min = Math.min(value, min);
            max = Math.max(value, max);

            double delta = weight * (value - mean);
            mean = mean + delta / duration;
            sigmaSum = sigmaSum + delta * (value - mean);
            updateIntegration(value, weight);
        }
    }

    /**
     * This updates the integration statistic using the Kahan/Neumaier summation
     * algorithm.
     */
    private void updateIntegration(double value, double weight) {
        double v = value * weight;
        double t = integration + v;
        if (Math.abs(integration) >= Math.abs(v)) {
            correction += (integration - t) + v;
        } else {
            correction += (v - t) + integration;
        }
        integration = t;
    }

    /**
     * Get the minimum value thus far.
     *
     * @return The minimum value of the channel history or null if the statistic
     * is invalid
     */
    public Double getMin() {
        if (!statsValid()) {
            return null;
        }
        return min;
    }

    /**
     * Get the maximum value thus far.
     *
     * @return The maximum value of the channel history or null if the statistic
     * is invalid
     */
    public Double getMax() {
        if (!statsValid()) {
            return null;
        }
        return max;
    }

    /**
     * Get the mean of the values seen so far
     *
     * @return The mean (average) value of the channel history or null if the
     * statistic is invalid
     */
    public Double getMean() {
        if (!statsValid()) {
            return null;
        }
        return mean;
    }

    /**
     * Get the standard deviation of the values seen so far. Note: This provides
     * no bias correction. Users with knowledge of the underlying EPICS data may
     * wish to apply a bias correction to sampling, e.g., multiplying by N /
     * (N-1).
     *
     * @return The variance of the channel history or null if the statistic is
     * invalid
     */
    public Double getSigma() {
        if (duration == 0 || !statsValid()) {
            return null;
        }
        return Math.sqrt(sigmaSum / duration);
    }

    /**
     * Get the RMS of the values seen so far. This is a computed statistic
     * equivalent to Math.sqrt(sigma*sigma + mean * mean);
     *
     * @return The RMS of the channel history or null if the statistic is
     * invalid
     */
    public Double getRms() {
        if (duration == 0 || !statsValid()) {
            return null;
        }

        return Math.sqrt(sigmaSum / duration + mean * mean);
    }

    /**
     * Get the amount of time for which valid data was available in the MYA
     * channel history. This is generally not the same as the difference between
     * begin and end parameters of a query as it excludes time for which the
     * channel history last recorded a non Event.UPDATE event.
     *
     * @return The time duration of the channel history for which data was
     * available or null if the statistic is invalid
     */
    public Double getDuration() {
        if (!statsValid()) {
            return null;
        }
        return duration;
    }

    /**
     * Get the integrated value of the channel with respect to time.
     *
     * @return The result of integrating across the channel history for which
     * data was available or null if the statistic is invalid.
     */
    // Use the Neumaier summation algorithm
    public Double getIntegration() {
        if (!statsValid()) {
            return null;
        }
        return integration + correction;
    }

    /**
     * Get the number of events pushed to the RunningStatistics object.
     *
     * @return The number of events
     */
    public long getEventCount() {
        // The event count includes the artificial start and end events
        return eventCount;
    }

    /**
     * Get the number of events pushed to the RunningStatistics whose EventCode
     * was Event.UPDATE.
     *
     * @return The number of update events
     */
    public long getUpdateCount() {
        return updateCount;
    }

    /**
     * Obtain the event stats.
     * <p>
     * stats and their index (order) are specified in the constructor.
     * </p>
     * @return The stats
     */
    public double[] getEventStats() {
        double[] stats = new double[eventStatsMap.length];

        for(short i = 0; i < stats.length; i++) {
            switch(eventStatsMap[i]) {
                case INTEGRATION:
                    // We don't use getIntegration() method because it does boxing and also an isValidCheck
                    // If stats are not valid (first point) then integration = 0, which is what we want anyways.
                    stats[i] = integration + correction;
                    break;
            }
        }

        return stats;
    }
}
