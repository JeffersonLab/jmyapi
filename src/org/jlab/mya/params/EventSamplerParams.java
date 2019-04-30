package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 * Parameters for the improved sampler; similar to Basic, but performs
 * application layer filtering. This sampler is good for when you want a large
 * percent of the data in the result.
 *
 * @author slominskir
 */
public class EventSamplerParams extends BinnedSamplerParams {

    private final long count;

    public EventSamplerParams(Metadata metadata, Instant begin, Instant end, long limit, long count) {
        super(metadata, begin, end, limit);
        this.count = count;
    }

    public long getCount() {
        return count;
    }
}
