package org.jlab.mya.stream.wrapped;

import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.analysis.RunningStatistics;
import org.jlab.mya.event.AnalyzedFloatEvent;
import org.jlab.mya.event.FloatEvent;

import java.io.IOException;

/**
 * Wraps a FloatEventStream and provides FloatEvents that are analyzed as they stream by.
 *
 * @author slominskir
 */
public class FloatAnalysisStream extends WrappedEventStreamAdaptor<AnalyzedFloatEvent, FloatEvent> {

    private final RunningStatistics seriesStats;

    /**
     * Create a new FloatIntegrationStream by wrapping a FloatEventStream.
     * <p>
     * Desired stats are specified via a statsMap which instructs the analysis service which stats to track
     * and the order (index) in which to return them.  Valid values are defined as constants in the RunningStatistics
     * class and include INTEGRATION.
     * </p>
     *
     * @param stream The FloatEventStream to wrap
     * @param statsMap The stat name and index of requested statistics
     */
    public FloatAnalysisStream(EventStream<FloatEvent> stream, short[] statsMap) {
        super(stream);
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
     * Get latest running statistics on this FloatIntegrationStream.
     *
     * @return The RunningStatistics
     */
    public RunningStatistics getLatestStats() {
        return seriesStats; // Should we return an immutable copy instead?
    }
}
