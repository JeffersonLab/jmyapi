package org.jlab.mya.stream.wrapped;

import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.analysis.EventStats;
import org.jlab.mya.analysis.RunningStatistics;
import org.jlab.mya.event.AnalyzedFloatEvent;
import org.jlab.mya.event.FloatEvent;

import java.io.IOException;

/**
 * Wraps a FloatEventStream and provides FloatEvents that are analyzed as they stream by.
 *
 * @author slominskir
 */
public class FloatAnalysisStream extends WrappedEventStreamAdaptor<FloatEvent, FloatEvent> {

    private final RunningStatistics seriesStats = new RunningStatistics();

    /**
     * Create a new FloatIntegrationStream by wrapping a FloatEventStream.
     *
     * @param stream The FloatEventStream to wrap
     */
    public FloatAnalysisStream(EventStream<FloatEvent> stream) {
        super(stream);
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
            EventStats eventStats = seriesStats.getEventStats();

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
