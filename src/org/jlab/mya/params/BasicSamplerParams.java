package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;
import org.jlab.mya.QueryParams;

/**
 * Parameters for the basic sampler (mySampler).
 * 
 * @author slominskir
 */
public class BasicSamplerParams extends QueryParams {
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
    public BasicSamplerParams(Metadata metadata, Instant begin, long stepMilliseconds, long sampleCount) {
        super(metadata, begin, begin.plusMillis(stepMilliseconds * sampleCount));
        this.stepMilliseconds = stepMilliseconds;
        this.sampleCount = sampleCount;
    }

    public long getStepMilliseconds() {
        return stepMilliseconds;
    }

    public long getSampleCount() {
        return sampleCount;
    }
}
