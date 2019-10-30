package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 * Parameters for the advanced sampler; uses the largest triangle three bucket
 * (LTTB) algorithm described in "Downsampling Time Series for Visual
 * Representation" (Steinarsson, 2013).
 *
 * @author apcarp
 */
public class GraphicalEventBinSamplerParams {

    private final long count;
    private final long numBins;

    /**
     * Create a new GraphicalEventBinSamplerParams.
     *
     * @param numBins The number of bins
     * @param count The count
     */
    public GraphicalEventBinSamplerParams(long numBins, long count) {
        this.count = count;
        this.numBins = numBins;
    }

    /**
     * Return the count.
     * 
     * @return The count
     */
    public long getCount() {
        return count;
    }

    /**
     * Return the number of bins.
     * 
     * @return The number of bins
     */
    public long getNumBins() {
        return numBins;
    }
}
