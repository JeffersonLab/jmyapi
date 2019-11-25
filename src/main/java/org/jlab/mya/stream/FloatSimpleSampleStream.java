package org.jlab.mya.stream;

import org.jlab.mya.EventStream;
import org.jlab.mya.event.FloatEvent;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Wraps a EventStream and provides FloatEvents that are down-sampled as they stream by using a simple
 * algorithm.
 * <p>
 * This stream reads the full dataset from the database and returns a subset (performs application layer filtering).
 * </p>
 * <p>
 * This algorithm bins by count, not by date interval like many other
 * sampling algorithms. There are pros and cons to this, but it means the
 * event-based nature of the data is preserved and doesn't give periods of
 * calm/idle time as many samples and give more samples to busy activity
 * periods.
 * </p>
 * <p>
 * Another feature of this algorithm is it streams over the entire dataset
 * once instead of issuing n-queries (n = # of bins). There are pros and
 * cons to this as well. This algorithm will generally perform better than
 * issuing n-queries would if there are only a few points per bin.
 * </p>
 * <p>
 * Note: Users must figure out number of events per bin threshold in which
 * to use n-queries instead.
 * </p>
 * <p>
 * Note: This algorithm promises not to create new FloatEvents.  Instead events are simply filtered out.  This
 * means you can pass in FloatEvent subclasses such as AnalyzedFloatEvents and they'll come out untouched.
 * </p>
 * @author slominskir
 */
public class FloatSimpleSampleStream<T extends FloatEvent> extends WrappedStream<T, T> {

    private final long binSize;
    private final BigDecimal fractional;
    private BigDecimal fractionalCounter = BigDecimal.ZERO;

    /**
     * Create a new FloatSimpleEventBinStream by wrapping a FloatEventStream.
     *
     * @param stream The FloatEventStream to wrap
     * @param limit The number of bins
     * @param count The total number of events
     * @param type The type
     */
    public FloatSimpleSampleStream(EventStream<T> stream, long limit, long count, Class<T> type) {
        super(stream, type);

        if (count > limit && limit > 0) {
            this.binSize = count / limit;
            this.fractional = BigDecimal.valueOf((count % limit)
                    / (double) limit);
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
    public T read() throws IOException {
        T event = null;

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
