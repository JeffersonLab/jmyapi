package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;
import org.jlab.mya.QueryParams;

/**
 *
 * @author ryans
 */
public class NaiveSamplerParams extends QueryParams {
    private final long limit;

    public NaiveSamplerParams(Metadata metadata, Instant begin, Instant end, long limit) {
        super(metadata, begin, end);
        
        this.limit = limit;
    }

    public long getLimit() {
        return limit;
    }
}
