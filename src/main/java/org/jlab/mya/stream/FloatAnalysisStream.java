package org.jlab.mya.stream;

import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.RunningStatistics;
import org.jlab.mya.event.AnalyzedFloatEvent;
import org.jlab.mya.event.FloatEvent;

import java.io.IOException;

/**
 * Wraps an EventStream of FloatEvents and provides AnalyzedFloatEvents.
 *
 * @author slominskir
 */
public class FloatAnalysisStream extends WrappedStream<AnalyzedFloatEvent, FloatEvent> {

    private final RunningStatistics seriesStats;

    /**
     * Create a new FloatAnalysisStream that wraps an EventStream of FloatEvents and accumulates no event stats
     * (only final series stats).
     *
     * @param stream The FloatEvent EventStream to wrap
     */
    public FloatAnalysisStream(EventStream<FloatEvent> stream) {
        this(stream, new short[0]);
    }

    /**
     * Create a new FloatAnalysisStream that wraps an EventStream of FloatEvents and accumulates event stats as
     * specified in the statsMap parameter.
     * <p>
     * Desired stats are specified via a statsMap which instructs the analysis service which stats to track
     * and the order (index) in which to return them.  Valid values are defined as constants in the RunningStatistics
     * class and include INTEGRATION.
     * </p>
     *
     * @param stream The FloatEvent EventStream to wrap
     * @param statsMap The stat name and index of requested statistics
     */
    public FloatAnalysisStream(EventStream<FloatEvent> stream, short[] statsMap) {
        super(stream, AnalyzedFloatEvent.class);
        seriesStats = new RunningStatistics(statsMap);
    }

    /**
     * Read the next event from the stream. Generally you'll want to iterate
     * over the stream using a while loop.
     *
     * @return The next event or null if End-Of-Stream reached
     * @throws IOException If unable to read the next event
     */
    @Override
    public AnalyzedFloatEvent read() throws IOException {
        FloatEvent fEvent = wrapped.read();
        AnalyzedFloatEvent iEvent = null;

        //System.out.println("fEvent: " + fEvent);

        if(fEvent != null) {

            float value = fEvent.getValue();

            seriesStats.push(fEvent);

            long timestamp = fEvent.getTimestamp();
            EventCode code = fEvent.getCode(); // Should always be EventCode.UPDATE?
            double[] eventStats = seriesStats.getEventStats();

            iEvent = new AnalyzedFloatEvent(timestamp, code, value, eventStats);
        }

        return iEvent;
    }

    /**
     * Get latest running statistics on this FloatAnalysisStream.
     *
     * @return The RunningStatistics
     */
    public RunningStatistics getLatestStats() {
        return seriesStats; // Should we return an immutable copy instead?
    }
}
