package org.jlab.mya.params;

/**
 * Parameters for the simple application-level event-based sampler.  The FloatSimpleEventBinSampleStream is generally
 * superseded by the FloatGraphicalEventBinSampleStream.
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
