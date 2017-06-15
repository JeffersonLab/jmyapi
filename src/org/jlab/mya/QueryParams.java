package org.jlab.mya;

import java.time.Instant;

/**
 * The parameters required to query for events.
 * 
 * @author slominskir
 */
public class QueryParams {
    private final Metadata metadata;    
    private final Instant begin;
    private final Instant end;

    /**
     * Create a new QueryParams.
     * 
     * @param metadata The PV metadata
     * @param begin The begin instant
     * @param end The end instant
     */
    public QueryParams(Metadata metadata, Instant begin, Instant end) {
        this.metadata = metadata;
        this.begin = begin;
        this.end = end;
    }

    /**
     * Return the metadata.
     * 
     * @return The metadata
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Return the begin instant.
     * 
     * @return The begin instant
     */
    public Instant getBegin() {
        return begin;
    }

    /**
     * Return the end instant.
     * 
     * @return The end instant
     */
    public Instant getEnd() {
        return end;
    }
}
