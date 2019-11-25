package org.jlab.mya.params;

import java.time.Instant;

import org.jlab.mya.Event;
import org.jlab.mya.Metadata;
import org.jlab.mya.QueryParams;
import org.jlab.mya.nexus.DataNexus;

/**
 * The parameters required to query for events in an interval with an inclusive
 * begin timestamp and exclusive end timestamp.
 *
 * @author slominskir
 */
public class IntervalQueryParams<T extends Event> extends QueryParams<T> {

    private final Instant begin;
    private final Instant end;
    private final DataNexus.IntervalQueryFetchStrategy fetch;

    /**
     * Create a new IntervalQueryParams for all event types and with streaming fetch strategy.
     *
     * @param metadata The PV metadata
     * @param begin The begin instant
     * @param end The end instant
     */
    public IntervalQueryParams(Metadata<T> metadata, Instant begin, Instant end) {
        this(metadata, false, DataNexus.IntervalQueryFetchStrategy.STREAM, begin, end);
    }

    /**
     * Create a new IntervalQueryParams.
     *
     * @param metadata The PV metadata
     * @param updatesOnly true to include updates only, false for all event
     * types
     * @param fetch The fetch strategy
     * @param begin The begin instant
     * @param end The end instant
     */
    public IntervalQueryParams(Metadata<T> metadata, boolean updatesOnly, DataNexus.IntervalQueryFetchStrategy fetch, Instant begin, Instant end) {
        super(metadata, updatesOnly);
        this.fetch = fetch;
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

    /**
     * Return the fetch strategy.
     *
     * @return The strategy
     */
    public DataNexus.IntervalQueryFetchStrategy getFetchStrategy() {
        return fetch;
    }
}