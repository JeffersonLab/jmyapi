package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;
import org.jlab.mya.QueryParams;

/**
 *
 * @author ryans
 */
public class BasicSamplerParams extends QueryParams {
    private final long stepMilliseconds;
    private final long sampleCount;

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
