package org.jlab.mya.stream.wrapped;

import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.analysis.RunningStatistics;
import org.jlab.mya.event.FloatEvent;

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
    public FloatEvent read() throws IOException {
        FloatEvent fEvent = wrapped.read();
        FloatEvent iEvent = null;

        //System.out.println("fEvent: " + fEvent);

        if(fEvent != null) {
            stats.push(fEvent);

            long timestamp = fEvent.getTimestamp();
            EventCode code = fEvent.getCode(); // Should always be EventCode.UPDATE?
            float value = 0; // If stats invalid we should use value of zero?
            if (stats.getIntegration() != null) {
                value = stats.getIntegration().floatValue();
            }

            iEvent = new FloatEvent(timestamp, code, value);
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
