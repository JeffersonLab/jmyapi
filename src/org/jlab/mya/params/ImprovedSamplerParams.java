package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 *
 * @author ryans
 */
public class ImprovedSamplerParams extends NaiveSamplerParams {
    
    private final long count;
    
    public ImprovedSamplerParams(Metadata metadata, Instant begin, Instant end, long limit, long count) {
        super(metadata, begin, end, limit);
        this.count = count;
    }

    public long getCount() {
        return count;
    }
}
