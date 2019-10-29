package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;
import org.jlab.mya.QueryParams;

/**
 * The parameters required to query for events in an interval with an inclusive
 * begin timestamp and exclusive end timestamp.
 *
 * @author slominskir
 */
public class IntervalQueryParams extends QueryParams {

    private final Instant begin;
    private final Instant end;

    /**
     * Create a new IntervalQueryParams for all event types.
     *
     * @param metadata The PV metadata
     * @param begin The begin instant
     * @param end The end instant
     */
    public IntervalQueryParams(Metadata metadata, Instant begin, Instant end) {
        this(metadata, false, begin, end);
    }

    /**
     * Create a new IntervalQueryParams.
     *
     * @param metadata The PV metadata
     * @param updatesOnly true to include updates only, false for all event
     * types
     * @param begin The begin instant
     * @param end The end instant
     */
    public IntervalQueryParams(Metadata metadata, boolean updatesOnly, Instant begin, Instant end) {
        super(metadata, updatesOnly);
        this.begin = begin;
        this.end = end;
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
