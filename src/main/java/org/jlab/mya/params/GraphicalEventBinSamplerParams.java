package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 * Parameters for the graphical application-level event-based sampler; uses the largest triangle three bucket
 * (LTTB) algorithm described in "Downsampling Time Series for Visual
 * Representation" (Steinarsson, 2013).
 *
 * @author apcarp
 */
public class GraphicalEventBinSamplerParams {

    private final long numBins;
    private final long count;

    /**
     * Create a new GraphicalEventBinSamplerParams.
     *
     * @param numBins The number of bins
     * @param count The count
     */
    public GraphicalEventBinSamplerParams(long numBins, long count) {
        this.numBins = numBins;
        this.count = count;
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
