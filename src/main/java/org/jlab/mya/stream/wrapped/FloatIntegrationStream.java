package org.jlab.mya.stream.wrapped;

import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.analysis.RunningStatistics;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntegratedFloatEvent;

import java.io.IOException;

/**
 * Wraps a FloatEventStream and provides FloatEvents that are integrated as they stream by.
 *
 * @author slominskir
 */
public class FloatIntegrationStream extends WrappedEventStreamAdaptor<FloatEvent, FloatEvent> {

    private final RunningStatistics stats = new RunningStatistics();

    /**
     * Create a new FloatIntegrationStream by wrapping a FloatEventStream.
     *
     * @param stream The FloatEventStream to wrap
     */
    public FloatIntegrationStream(EventStream<FloatEvent> stream) {
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
    public IntegratedFloatEvent read() throws IOException {
        FloatEvent fEvent = wrapped.read();
        IntegratedFloatEvent iEvent = null;

        //System.out.println("fEvent: " + fEvent);

        if(fEvent != null) {

            float value = fEvent.getValue();

            stats.push(fEvent);

            long timestamp = fEvent.getTimestamp();
            EventCode code = fEvent.getCode(); // Should always be EventCode.UPDATE?
            double integratedValue = 0; // If stats invalid we should use value of zero?
            if (stats.getIntegration() != null) {
                integratedValue = stats.getIntegration();
            }

            iEvent = new IntegratedFloatEvent(timestamp, code, value, integratedValue);
        }

        return iEvent;
    }

    /**
     * Get latest running statistics on this FloatIntegrationStream.
     *
     * @return The RunningStatistics
     */
    public RunningStatistics getLatestStats() {
        return stats; // Should we return an immutable copy instead?
    }
}
