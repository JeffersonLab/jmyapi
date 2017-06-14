package org.jlab.mya;

import java.time.Instant;

/**
 *
 * @author ryans
 */
public class QueryParams {
    private final Metadata metadata;    
    private final Instant begin;
    private final Instant end;

    public QueryParams(Metadata metadata, Instant begin, Instant end) {
        this.metadata = metadata;
        this.begin = begin;
        this.end = end;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Instant getBegin() {
        return begin;
    }

    public Instant getEnd() {
        return end;
    }
}
