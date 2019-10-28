package org.jlab.mya.stream.wrapped;

import org.jlab.mya.EventStream;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.params.SimpleEventBinSamplerParams;
import org.jlab.mya.stream.FloatEventStream;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Wraps a FloatEventStream and provides FloatEvents that are down-sampled as they stream by using a simple
 * algorithm.
 *
 * This stream reads the full dataset from the database and returns a subset (performs application layer filtering).
 *
 * This algorithm bins by count, not by date interval like many other
 * sampling algorithms. There are pros and cons to this, but it means the
 * event-based nature of the data is preserved and doesn't give periods of
 * calm/idle time as many samples and give more samples to busy activity
 * periods.
 *
 * Another feature of this algorithm is it streams over the entire dataset
 * once instead of issuing n-queries (n = # of bins). There are pros and
 * cons to this as well. This algorithm will generally perform better than
 * issuing n-queries would if there are only a few points per bin.
 *
 * Note: Users must figure out number of events per bin threshold in which
 * to use n-queries instead.
 *
 * @author slominskir
 */
public class FloatSimpleEventBinSampleStream extends WrappedEventStreamAdaptor<FloatEvent, FloatEvent> {

    private final SimpleEventBinSamplerParams samplerParams;
    private final long binSize;
    private final BigDecimal fractional;
    private BigDecimal fractionalCounter = BigDecimal.ZERO;

    /**
     * Create a new FloatSimpleEventBinStream by wrapping a FloatEventStream.
     *
     * @param stream The FloatEventStream to wrap
     * @param params THe SimpleEventBinSamplerParams
     */
    public FloatSimpleEventBinSampleStream(EventStream<FloatEvent> stream, SimpleEventBinSamplerParams params) {
        super(stream);

        this.samplerParams = params;

        if (params.getCount() > params.getLimit() && params.getLimit() > 0) {
            this.binSize = params.getCount() / params.getLimit();
            this.fractional = BigDecimal.valueOf((params.getCount() % params.getLimit())
                    / Double.valueOf(params.getLimit()));
        } else {
            this.binSize = 1;
            this.fractional = BigDecimal.ZERO;
        }
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
        FloatEvent event = null;

        BigDecimal old = fractionalCounter;

        //System.out.println("old fractional counter: " + old);
        fractionalCounter = fractionalCounter.add(fractional);

        //System.out.println("new fractional counter: " + fractionalCounter);
        long effectiveBinSize;

        //System.out.println("truncated old: " + old.setScale(0, BigDecimal.ROUND_DOWN));
        //System.out.println("truncated new: " + fractionalCounter.setScale(0, BigDecimal.ROUND_DOWN));
        if (old.setScale(0, RoundingMode.DOWN).compareTo(fractionalCounter.setScale(0,
                RoundingMode.DOWN)) == 0) {
            effectiveBinSize = binSize;
        } else {
            effectiveBinSize = binSize + 1;
        }

        //System.out.println("effectiveBinSize: " + effectiveBinSize);
        for (int i = 0; i < effectiveBinSize; i++) {
            event = wrapped.read();

            if (event == null) {
                break;
            }
        }

        return event;
    }
}
