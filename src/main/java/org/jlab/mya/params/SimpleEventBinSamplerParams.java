package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;

/**
 * Parameters for the simple event bin sampler; performs
 * application layer filtering.
 *
 * @author slominskir
 */
public class SimpleEventBinSamplerParams {

    private final long limit;
    private final long count;

    public SimpleEventBinSamplerParams(long limit, long count) {
        this.limit = limit;
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public long getLimit() {
        return limit;
    }
}
