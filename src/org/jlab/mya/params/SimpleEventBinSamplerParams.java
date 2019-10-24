package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 * Parameters for the simple event bin sampler; performs
 * application layer filtering.
 *
 * @author slominskir
 */
public class SimpleEventBinSamplerParams extends MyGetSampleParams {

    private final long count;

    public SimpleEventBinSamplerParams(Metadata metadata, Instant begin, Instant end, long limit, long count) {
        super(metadata, begin, end, limit);
        this.count = count;
    }

    public long getCount() {
        return count;
    }
}
