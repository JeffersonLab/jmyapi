package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 * Parameters for the MYA mySampler algorithm (mySampler).
 * 
 * @author slominskir
 */
public class MySamplerParams extends IntervalQueryParams {
    private final long stepMilliseconds;
    private final long sampleCount;

    /**
     * Create a new BasicSamplerParams.
     * 
     * @param metadata The PV metadata
     * @param begin The begin instant
     * @param stepMilliseconds The step size in milliseconds
     * @param sampleCount The number of samples
     */   
    public MySamplerParams(Metadata metadata, Instant begin, long stepMilliseconds, long sampleCount) {
        super(metadata, begin, begin.plusMillis(stepMilliseconds * sampleCount));
        this.stepMilliseconds = stepMilliseconds;
        this.sampleCount = sampleCount;
    }

    /**
     * Return the step milliseconds.
     * 
     * @return The step milliseconds
     */
    public long getStepMilliseconds() {
        return stepMilliseconds;
    }

    /**
     * Return the sample count.
     * 
     * @return The sample count
     */
    public long getSampleCount() {
        return sampleCount;
    }
}
