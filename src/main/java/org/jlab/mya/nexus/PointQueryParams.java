package org.jlab.mya.nexus;

import java.time.Instant;

import org.jlab.mya.event.Event;
import org.jlab.mya.Metadata;

/**
 * The parameters required to query for a single event near a point in time.
 *
 * @author slominskir
 */
class PointQueryParams<T extends Event> extends QueryParams<T> {

    private final Instant timestamp;
    private final boolean lessThan;
    private final boolean orEqual;

    /**
     * Create a new PointQueryParams with the default behavior of searching for
     * the event occurring at a time less than or equal to the timestamp
     * provided and including all event types.
     *
     * @param metadata The PV metadata
     * @param timestamp The point in time
     */
    public PointQueryParams(Metadata<T> metadata, Instant timestamp) {
        this(metadata, false, timestamp, true, true);
    }

    /**
     * Create a new PointQueryParams.
     *
     * @param metadata The PV metadata
     * @param updatesOnly true to include updates only, false for all event
     * types
     * @param timestamp The point in time
     * @param lessThan true if an event less than the point-in-time, false for
     * an event greater than the point-in-time.
     * @param orEqual true if the point exactly at the given timestamp is
     * returned, false if the timestamp is exclusive
     */
    public PointQueryParams(Metadata<T> metadata, boolean updatesOnly, Instant timestamp, boolean lessThan, boolean orEqual) {
        super(metadata, updatesOnly);
        this.timestamp = timestamp;
        this.lessThan = lessThan;
        this.orEqual = orEqual;
    }

    /**
     * Return the point-in-time to search.
     *
     * @return The point-in-time timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * Return true if the event search looks for the last event before the
     * point-in-time or false if the event search looks for the first event
     * after the point in time.
     *
     * @return true if the direction of the search is most recent value up to
     * the point-in-time
     */
    public boolean isLessThan() {
        return lessThan;
    }

    /**
     * Return true if the event search looks for the event exactly at the
     * point-in-time or false if that point is excluded.
     *
     * @return true if the exact point-in-time is included
     */
    public boolean isOrEqual() {
        return orEqual;
    }
}
