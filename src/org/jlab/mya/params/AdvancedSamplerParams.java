package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 *
 * @author ryans
 */
public class AdvancedSamplerParams extends IntervalQueryParams {
    
    private final long count;
    private final long numBins;
    
    public AdvancedSamplerParams(Metadata metadata, Instant begin, Instant end, long numBins, long count) {
        super(metadata, begin, end);
        this.count = count;
        this.numBins = numBins;
    }

    public long getCount() {
        return count;
    }

    public long getNumBins() {
        return numBins;
    }    
}
