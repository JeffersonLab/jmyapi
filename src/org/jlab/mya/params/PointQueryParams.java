package org.jlab.mya.params;

import java.time.Instant;
import org.jlab.mya.Metadata;
import org.jlab.mya.QueryParams;

/**
 * The parameters required to query for a single event near a point in time.
 *
 * @author slominskir
 */
public class PointQueryParams extends QueryParams {

    private final Instant timestamp;
    private final boolean lessThanOrEqual;

    /**
     * Create a new PointQueryParams with the default behavior of searching for the event occurring
     * at a time less than or equal to the timestamp provided.
     *
     * @param metadata The PV metadata
     * @param timestamp The point in time
     */
    public PointQueryParams(Metadata metadata, Instant timestamp) {
        this(metadata, timestamp, true);
    }

    /**
     * Create a new PointQueryParams.
     *
     * @param metadata The PV metadata
     * @param timestamp The point in time
     * @param lessThanOrEqual true an event less than or equal to the point-in-time, false for an
     * event greater than or equal to the point-in-time.
     */
    public PointQueryParams(Metadata metadata, Instant timestamp, boolean lessThanOrEqual) {
        super(metadata);
        this.timestamp = timestamp;
        this.lessThanOrEqual = lessThanOrEqual;
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
     * Return true if the event search looks for the last event before (or equal) the point-in-time
     * or false if the event search looks for the first event after (or equal) the point in time.
     *
     * @return true if the direction of the search is most recent value up to the point-in-time
     */
    public boolean isLessThanOrEqual() {
        return lessThanOrEqual;
    }
}
