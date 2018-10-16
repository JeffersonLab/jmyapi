package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 * Parameters for the naive sampler (myget -l).
 *
 * @author slominskir
 */
public class NaiveSamplerParams extends IntervalQueryParams {

    private final long limit;

    /**
     * Create a new NaiveSamplerParams.
     * 
     * @param metadata The PV metadata
     * @param begin The begin instant
     * @param end The end instant
     * @param limit The limit
     */
    public NaiveSamplerParams(Metadata metadata, Instant begin, Instant end, long limit) {
        super(metadata, begin, end);

        this.limit = limit;
    }

    /**
     * Return the event limit.
     *
     * @return The limit
     */
    public long getLimit() {
        return limit;
    }
}